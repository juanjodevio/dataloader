# Data Load Engine – Project Debrief & System Architecture

Recipe-Driven Data Loading Framework (Python + Optional Rust Core)

## 1. Project Summary

This project defines a data loading engine built around declarative recipes, enabling robust EL workflows with minimal code. Recipes describe:

- The source (Postgres, S3, API, etc.)
- Any transformations (rename, cast, enrich, audit)
- The destination (Redshift, Snowflake, S3, DuckDB, etc.)
- Incremental logic with cursor/watermark recovery
- Runtime behavior such as batching, retries, parallelism

The library delivers:

- A clean Python developer experience
- A declarative configuration style similar to Chef cookbooks
- Stateful incremental loads
- A pluggable connector system
- Optional Rust acceleration for performance-critical operations

The end goal: define what you want to sync, not how, and the engine handles reliability, batching, retries, and state.

## 2. High-Level Goals

- Declarative recipe-based data pipelines
- Idempotent & incremental loads
- Reusable base recipes via `extends:`
- Transform pipelines (Python/Rust hybrid)
- Clean DB-agnostic connector abstraction
- CLI + Python API
- Optional Arrow/Polars Rust core

## 3. Example Recipe

```yaml
name: load_customers_from_postgres_to_redshift

extends: base_recipe.yaml

source:
  type: postgres
  host: "{{ env.DB_HOST }}"
  database: "{{ env.DB_NAME }}"
  user: "{{ env.DB_USER }}"
  password: "{{ env.DB_PASSWORD }}"
  table: public.customers
  incremental:
    strategy: cursor
    cursor_column: updated_at

transform:
  steps:
    - type: rename_columns
      mapping:
        fname: first_name
        lname: last_name

destination:
  type: redshift
  host: "{{ env.DW_HOST }}"
  database: "{{ env.DW_DB }}"
  table: dw.customers
  write_mode: merge
  merge_keys: [id]

runtime:
  batch_size: 5000   # override base
  parallelism: 4
```

## 4. System Architecture

```
        ┌────────────────┐
        │  Recipe (YAML) │
        └───────▲────────┘
                │ Parsing & Validation
                ▼
     ┌────────────────────────┐
     │   Recipe Model Layer   │
     │ (Pydantic-based schema)│
     │  + Template Rendering  │
     │  + Inheritance Merger  │
     └───────────┬────────────┘
                 │ Build Execution Plan
                 ▼
     ┌──────────────────────────┐
     │     Execution Engine     │
     ├─────────┬────────────────┤
     │ State   │ Transform      │
     │ Mgmt    │ Pipeline       │
     └─────┬───┴─────────┬──────┘
           │           │
           ▼           ▼
   ┌──────────────┐ ┌───────────────┐
   │ Source Plug- │ │ Destination   │
   │     ins      │ │    Plug-ins   │
   └──────┬───────┘ └───────┬───────┘
          │                  │
          ▼                  ▼
     Data Stream ---> Transform ---> Write
```

## 5. Core Components

### 5.1 Recipe Model Layer ✅ Implemented

Responsible for:

- Parsing YAML/JSON via `RecipeLoader`
- Schema validation via Pydantic models
- Applying inheritance via `extends:` with `RecipeMerger`
- Template rendering (`{{ env.VAR }}`, `{{ var.NAME }}`, `{{ recipe.name }}`)
- Delete semantics for inheritance overrides
- Constructing execution plans

**Implemented Models:**

| Model | File | Description |
|-------|------|-------------|
| `Recipe` | `models/recipe.py` | Root recipe with name, extends, source, destination, transform, runtime |
| `SourceConfig` | `models/source_config.py` | Source configuration with type-specific validation |
| `DestinationConfig` | `models/destination_config.py` | Destination configuration with write modes |
| `TransformConfig` | `models/transform_config.py` | Transform pipeline with steps |
| `RuntimeConfig` | `models/runtime_config.py` | Batch size, retries, parallelism |
| `IncrementalConfig` | `models/source_config.py` | Cursor-based incremental strategy |

**Template System:**

```python
# Supported template patterns
"{{ env_var('DB_HOST') }}"      # Environment variables (function call)
"{{ var('table_name') }}"        # CLI-provided variables (function call)
"{{ recipe.name }}"              # Recipe metadata (dot notation)
```

Templates are rendered during recipe loading, allowing connection parameters and other values to be injected from environment variables or CLI arguments. All connection configuration is specified directly in recipes using templates, not via separate connection dictionaries.

### 5.2 Connectors – Unified Plug-in Architecture ✅ Implemented

**Unified Connector Protocol** (`connectors/base.py`):

Connectors are unified into a single `Connector` protocol that can handle both read and write operations:

```python
@runtime_checkable
class Connector(Protocol):
    def read_batches(self, state: State) -> Iterable[Batch]: ...
    def write_batch(self, batch: Batch, state: State) -> None: ...
```

Connectors can implement read-only, write-only, or both operations depending on their capabilities.

**Write Dispositions:**

Connectors that support write operations support three write dispositions:

- **`append`**: Add new data to existing tables/files without removing existing data
- **`overwrite`**: Replace all existing data in the target table/file
- **`merge`**: Update existing rows based on merge keys and insert new rows (upsert operation)

The write disposition is specified in the destination configuration via the `write_mode` field. Not all connectors support all three modes (e.g., file-based connectors typically support `append` and `overwrite`, while database connectors may support all three).

**Registry Pattern** (`connectors/registry.py`):

```python
# Registration via decorator (preferred)
@register_connector("postgres")
def create_postgres_connector(config):
    return PostgresConnector(config)

# Or direct call
register_connector("duckdb", create_duckdb_connector)

# Retrieval
connector = get_connector("postgres", config)
```

**Implemented Connectors:**

| Connector | Stack | Features | Operations |
|-----------|-------|----------|------------|
| `PostgresConnector` | SQLAlchemy + psycopg2 | Streaming results, cursor-based incremental, schema introspection, batch inserts | Read/Write |
| `DuckDBConnector` | DuckDB | File-based or in-memory databases, automatic schema creation, append/overwrite/merge modes, query support | Read/Write |
| `FileStoreConnector` | fsspec + format handlers | Unified file storage abstraction, multiple backends (S3, local), multiple formats (CSV, JSON, JSONL, Parquet), incremental by modification time | Read/Write |

**Technology Choices:**

- **Databases (Postgres, RDS):** SQLAlchemy for dialect abstraction
  - Supports Postgres, MySQL, Redshift via dialect parameter
  - Connection pooling, streaming results
  - Schema introspection via `inspect()`

- **File Storage (S3, Local, Azure, GCS):**
  - **fsspec** for unified filesystem abstraction (clean file-like interface)
  - **Format handlers** for extensible format support (CSV, JSON, JSONL, Parquet)
  - Backend-specific storage options (S3 credentials, Azure credentials, etc.)
  - Automatic backend detection from URL schemes (s3://, file://, etc.)

**FileStore Connector Architecture:**

The `FileStoreConnector` provides a unified interface for file-based storage:

- **Backends**: S3, local filesystem (Azure, GCS, SFTP can be added)
- **Formats**: CSV, JSON, JSONL, Parquet (extensible via format registry)
- **Format Handlers**: Pluggable format system using `Format` ABC
- **Custom Formats**: Users can register custom format handlers via `@register_format` decorator

**Cross-Platform Path Handling:**

The FileStore connector ensures cross-platform compatibility by converting `pathlib.Path` objects to strings before passing them to fsspec operations. This is critical on Windows where `WindowsPath` objects must be explicitly converted to strings for filesystem operations. The connector handles this conversion internally, so users can safely use either string paths or `pathlib.Path` objects in recipe configurations.

### 5.3 Transform Pipeline ✅ Implemented

Sequential pipeline executor that applies transform steps to batches.

**Pipeline Executor** (`transforms/pipeline.py`):

```python
from dataloader.transforms import TransformPipeline
from dataloader.models.transform_config import TransformConfig

pipeline = TransformPipeline(recipe.transform)
transformed_batch = pipeline.apply(batch)
```

**Transform Registry** (`transforms/registry.py`):

```python
# Registration via decorator
@register_transform("rename_columns")
def create_rename_transform(config):
    return RenameColumnsTransform(config)

# Retrieval
transform = get_transform("rename_columns", {"mapping": {"old": "new"}})
```

**Implemented Transforms:**

| Transform | Config | Description |
|-----------|--------|-------------|
| `rename_columns` | `mapping: {old: new}` | Rename columns in batch |
| `cast` | `columns: {col: type}` | Cast column types (str, int, float, datetime) |
| `add_column` | `name`, `value` | Add column with constant or template value |

**Example Pipeline:**

```yaml
transform:
  steps:
    - type: rename_columns
      mapping:
        fname: first_name
        lname: last_name
    - type: cast
      columns:
        age: int
        created_at: datetime
    - type: add_column
      name: source
      value: "{{ recipe.name }}"
```

**Features:**
- Sequential step execution with fail-fast error handling
- Batch validation after each step
- Metadata preservation through transforms
- Template support in `add_column` values
- Extensible via `@register_transform` decorator

### 5.4 State Management ✅ Implemented

**State Model** (`core/state.py`):

```python
class State(BaseModel):
    cursor_values: dict[str, Any]   # Last processed cursor per column
    watermarks: dict[str, Any]      # High watermarks
    checkpoints: list[dict]         # Recovery checkpoints
    metadata: dict[str, Any]        # Additional state metadata
```

**State Backend Protocol** (`core/state_backend.py`):

```python
class StateBackend(Protocol):
    def load(self, recipe_name: str) -> dict[str, Any]: ...
    def save(self, recipe_name: str, state: dict[str, Any]) -> None: ...
```

**Implemented Backends:**
- Local JSON ✅ Implemented (`LocalStateBackend`)
  - Stores state in JSON files under `.state/{recipe_name}.json`
  - Uses atomic writes to prevent corruption
  - Automatically creates state directory if needed

**Planned backends:**
- S3
- DynamoDB
- SQL table backend

### 5.5 Batch Format ✅ Implemented

**Batch Protocol** (`core/batch.py`):

```python
class Batch(Protocol):
    @property
    def columns(self) -> list[str]: ...
    
    @property
    def rows(self) -> list[list[Any]]: ...
    
    @property
    def metadata(self) -> dict[str, Any]: ...
    
    @property
    def row_count(self) -> int: ...
    
    def to_dict(self) -> dict[str, Any]: ...
```

**ArrowBatch Implementation** (Current):

All batches use Apache Arrow format via PyArrow for:
- Zero-copy data transfer between connectors
- Memory-efficient processing
- Better performance for large datasets
- Native integration with Arrow-based tools (Polars, DuckDB, etc.)

```python
# Create ArrowBatch from Arrow table
import pyarrow as pa
table = pa.table({"id": [1, 2], "name": ["Alice", "Bob"]})
batch = ArrowBatch(table, metadata={"source_type": "postgres", "table": "users"})

# Or create from rows (factory method)
batch = ArrowBatch.from_rows(
    columns=["id", "name", "updated_at"],
    rows=[[1, "Alice", "2024-01-01"], [2, "Bob", "2024-01-02"]],
    metadata={"source_type": "postgres", "table": "users"}
)

# Access underlying Arrow table for zero-copy operations
arrow_table = batch.to_arrow()
```

### 5.6 Exception Hierarchy ✅ Implemented

```python
DataLoaderError          # Base exception
├── RecipeError          # Recipe parsing/validation failures
├── ConnectorError       # Connector operations failures
├── TransformError       # Transform execution failures
├── StateError           # State backend operations failures
└── EngineError          # Execution engine failures
```

All exceptions include structured `context` dict for debugging.

### 5.7 Execution Engine ✅ Implemented

Core execution loop implemented in `core/engine.py`:

```python
def execute(recipe: Recipe, state_backend: StateBackend) -> None:
    state = State.from_dict(state_backend.load(recipe.name))
    source_connector = _get_connector(recipe.source)
    transformer = _get_transformer(recipe.transform)
    destination_connector = _get_connector(recipe.destination)
    
    for batch in source_connector.read_batches(state):
        batch = transformer.apply(batch)
        destination_connector.write_batch(batch, state)
        state_backend.save(recipe.name, state.to_dict())
```

**Features:**
- Loads state for recipe before execution
- Creates source connector, transformer, and destination connector from recipe config
- Processes batches sequentially
- Applies transforms to each batch
- Writes batches to destination connector
- Saves state after each batch for resumability
- All connection parameters come from recipe (templates rendered during loading)
- Comprehensive error handling with context
- Unified connector interface (same connector can be used as source or destination)

## 6. Rust Engine (Optional) 🔮 Future (v0.10)

Rust acceleration is planned for performance-critical operations:

- Native Rust Arrow implementation
- Rust-based transform engine
- CSV/Parquet read/write at high throughput
- IO parallel orchestration without Python GIL
- Bindings via PyO3

See roadmap section for details.

## 7. Developer Experience

### 7.1 Python API ✅ Implemented

**Public API** (`api.py`):

```python
from dataloader import from_yaml, run_recipe, run_recipe_from_yaml, LocalStateBackend

# Load recipe from YAML (templates rendered automatically)
recipe = from_yaml("examples/recipes/customers.yaml")

# Execute recipe with state backend
state_backend = LocalStateBackend(".state")
run_recipe(recipe, state_backend)

# Or use convenience function
run_recipe_from_yaml("examples/recipes/customers.yaml", state_dir=".state")
```

**Connection Configuration:**

All connection parameters are specified in recipes using Jinja2-style templates:

```yaml
source:
  type: postgres
  host: "{{ env_var('DB_HOST') }}"
  user: "{{ env_var('DB_USER') }}"
  password: "{{ env_var('DB_PASSWORD') }}"
  table: public.customers
```

Templates are rendered during recipe loading, so no separate connection dictionaries are needed.

**Package Exports** (`dataloader/__init__.py`):

- Public API: `from_yaml`, `run_recipe`, `run_recipe_from_yaml`
- Core classes: `Recipe`, `State`, `StateBackend`, `LocalStateBackend`, `Batch`, `ArrowBatch`
- Exceptions: `DataLoaderError`, `RecipeError`, `ConnectorError`, `TransformError`, `StateError`, `EngineError`

### 7.2 CLI 🚧 Planned

```bash
dataloader init
dataloader run recipe.yaml
dataloader validate recipe.yaml
dataloader show-state <recipe>
dataloader list-connectors
```

## 8. Roadmap

| # | Milestone | Status | Priority | Description |
|---|-----------|--------|----------|-------------|
| 1 | [Prototype](#prototype--complete) | ✅ Complete | - | Recipe model, connectors, transforms, execution engine |
| 2 | [Reliable MVP](#reliable-mvp--complete) | ✅ Complete | - | Parallelism, logging, metrics, state backends, CLI |
| 3 | [Optional Dependencies](#optional-dependencies--complete) | ✅ Complete | - | Dependency management with optional extras |
| 4 | [Arrow Batch Support](#arrow-batch-support--complete) | ✅ Complete | - | Apache Arrow batch format for improved performance |
| 5 | [Schema Management & Type System](#schema-management--type-system) | ✅ Complete | High | Automatic schema inference, evolution, validation, registry, and recipe config |
| 6 | [Data Normalization](#data-normalization) | 🚧 Planned | Medium | Automatically flatten nested data structures |
| 7 | [Incremental Loading & State Management](#incremental-loading--state-management) | 🚧 Planned | High | Robust incremental loading with cursor-based and watermark strategies |
| 8 | [Additional File Storage Backends](#additional-file-storage-backends) | 🚧 Planned | Medium | SFTP, Azure Blob Storage, Google Cloud Storage |
| 9 | [Comprehensive Integration Tests](#comprehensive-integration-tests) | 🚧 Planned | High | Comprehensive integration test suite covering all connectors and scenarios |
| 10 | [Top 10 Datastores Connectors](#top-10-datastores-connectors) | 🚧 Planned | Highest | MySQL, Snowflake, BigQuery, Redshift, SQL Server, Databricks, Oracle, MongoDB, ClickHouse, Elasticsearch |
| 11 | [Built-in SaaS Source Connectors](#built-in-saas-source-connectors) | 🚧 Planned | High | Pre-built connectors for Stripe, Shopify, Salesforce, etc. |
| 12 | [Production Features](#production-features) | 🚧 Planned | Essential | Enterprise-ready observability, scheduling, and security |
| 13 | [Rust Engine](#rust-engine) | 🚧 Planned | Future | Optional Rust acceleration for high-performance scenarios |

### 1. Prototype ✅ Complete

**Version**: 0.0.0b1

All features completed. See [CHANGELOG.md](CHANGELOG.md) for detailed release notes.

### 2. Reliable MVP ✅ Complete

**Version**: 0.0.0b2

All features completed. See [CHANGELOG.md](CHANGELOG.md) for detailed release notes.

### 3. Optional Dependencies ✅ Complete

**Version**: 0.0.0b3

All features completed. See [CHANGELOG.md](CHANGELOG.md) for detailed release notes.

### 4. Arrow Batch Support ✅ Complete

**Version**: 0.0.0b4

All features completed. See [CHANGELOG.md](CHANGELOG.md) for detailed release notes.

### 5. Schema Management & Type System ✅ Complete

**Version**: 0.0.0b5

All features completed. See [CHANGELOG.md](CHANGELOG.md) for detailed release notes.

**Priority**: High - Foundation for robust data handling


### 6. Data Normalization

**Priority**: Medium - Useful for nested data sources

**Goal**: Automatically flatten nested data structures for easier analysis

- [ ] **Nested Data Flattening**
  - Automatically flatten nested JSON/dicts
  - Create child tables for arrays
  - Preserve relationships (foreign keys)
  - Add `NormalizationEngine` class

- [ ] **Normalization Configuration**
  - Control flattening behavior
  - Name child tables
  - Set normalization depth
  - Configure array handling strategies

- [ ] **Data Structure Analysis**
  - Detect nested structures
  - Suggest normalization strategies
  - Preview normalized schema

**Example Recipe Enhancement:**
```yaml
normalization:
  enabled: true
  max_depth: 3
  strategy: auto  # or: flatten, explode, json_column
  array_handling:
    - field: orders
      action: create_table
      table_name: customer_orders
      relationship: one_to_many
```

### 7. Incremental Loading & State Management

**Priority**: High - Critical for efficient data pipelines

**Goal**: Robust incremental loading with cursor-based and watermark strategies

- [ ] **Incremental Loading Strategies**
  - Cursor-based incremental loading (basic support exists, needs enhancement)
  - Watermark-based incremental loading
  - Change data capture (CDC) support
  - Timestamp-based incremental loading
  - Custom incremental strategies

- [ ] **State Management Enhancements**
  - Robust state persistence across runs
  - State recovery after failures
  - State validation and migration
  - State backend improvements (S3, DynamoDB)
  - State compression for large datasets

- [ ] **Incremental Loading by Connector**
  - Postgres: Cursor-based incremental (SQLAlchemy)
  - FileStore: Modification time-based incremental
  - DuckDB: Cursor-based incremental
  - S3: Object listing with timestamps
  - All connectors: Unified incremental interface

- [ ] **Error Recovery**
  - Automatic retry with exponential backoff
  - Resume from last successful batch
  - State checkpointing
  - Partial failure handling
  - Dead letter queue for failed batches

**Example Recipe:**
```yaml
source:
  type: postgres
  table: customers
  incremental:
    strategy: cursor
    cursor_column: updated_at
    initial_value: "2024-01-01 00:00:00"

destination:
  type: duckdb
  database: customers.duckdb
  table: customers
  write_mode: merge
  merge_keys: [id]
```

### 8. Additional File Storage Backends

**Priority**: Medium - Expand storage options

**Goal**: Support additional file storage backends beyond S3 and local filesystem

- [ ] **SFTP Backend**
  - SFTP connector using paramiko or pysftp
  - Support for SSH key and password authentication
  - Directory listing and file operations

- [ ] **Azure Blob Storage**
  - Azure Blob Storage connector using azure-storage-blob
  - Support for Azure-specific features (containers, snapshots)
  - Authentication via connection string or service principal

- [ ] **Google Cloud Storage**
  - GCS connector using google-cloud-storage
  - Support for GCS-specific features (buckets, objects)
  - Authentication via service account or application default credentials

**Implementation Approach:**
- Leverage fsspec for unified filesystem abstraction
- Follow FileStoreConnector pattern
- Support all existing formats (CSV, JSON, JSONL, Parquet)
- Maintain consistent API across all backends

### 9. Comprehensive Integration Tests

**Priority**: High - Essential for reliability and confidence

**Goal**: Comprehensive integration test suite covering all connectors, formats, and pipeline scenarios

- [ ] **Core Integration Tests**
  - End-to-end pipeline tests (CSV → DuckDB, JSON → Parquet, etc.)
  - Multi-connector pipeline tests (FileStore → FileStore → DuckDB)
  - Format conversion tests (CSV → JSON → Parquet, all combinations)
  - Recipe loading and inheritance tests
  - Template rendering tests with real recipes

- [ ] **Connector-Specific Integration Tests**
  - Postgres connector: Read/write with real database
  - DuckDB connector: File-based and in-memory databases
  - FileStore connector: Local filesystem and S3 backends
  - All format handlers: CSV, JSON, JSONL, Parquet
  - Cross-connector compatibility tests

- [ ] **Transform Integration Tests**
  - Transform pipeline with multiple steps
  - Transform combinations (rename + cast + add_column)
  - Transform error handling
  - Transform with different data types

- [ ] **State Management Integration Tests**
  - State persistence across runs
  - State backend isolation between recipes
  - State recovery after failures
  - Multiple state backends (Local, S3, DynamoDB)

- [ ] **Error Handling Integration Tests**
  - Connection failures
  - Invalid data handling
  - Schema mismatches
  - Partial failures and recovery

- [ ] **Performance & Scale Tests**
  - Large dataset handling
  - Memory efficiency tests
  - Batch size optimization tests
  - Parallel execution tests

- [ ] **CI/CD Integration**
  - Automated test execution
  - Test coverage reporting
  - Integration test fixtures and setup
  - Docker-based test environments for databases

**Test Infrastructure:**
- Test fixtures for all connectors
- Mock services for external dependencies
- Test data generators
- Performance benchmarking framework

### 10. Top 10 Datastores Connectors

**Priority**: Highest - Critical for adoption and utility

**Goal**: Support the most popular datastores to maximize utility and adoption

This milestone focuses on implementing connectors for the most widely-used data platforms and databases. Each connector will follow the unified connector protocol and support read/write operations where applicable.

**Priority Datastores:**

1. **MySQL** - Most popular open-source relational database
   - SQLAlchemy-based connector (similar to Postgres)
   - Support MySQL-specific types and features
   - Incremental loading support

2. **Snowflake** - Leading cloud data warehouse
   - Native Snowflake connector with snowflake-connector-python
   - Support for Snowflake-specific features (stages, warehouses, etc.)
   - Zero-copy operations with Arrow batches

3. **BigQuery** - Google Cloud data warehouse
   - google-cloud-bigquery connector
   - Support for BigQuery-specific features (partitioning, clustering)
   - Efficient streaming inserts

4. **Redshift** - AWS data warehouse (SQLAlchemy-compatible)
   - SQLAlchemy-based connector
   - Support Redshift-specific optimizations
   - COPY commands for bulk loads

5. **SQL Server** - Microsoft's enterprise database
   - SQLAlchemy-based connector
   - Support for SQL Server-specific types
   - Incremental loading support

6. **Databricks** - Unified analytics platform
   - Databricks SQL connector
   - Support for Delta Lake format
   - Unity Catalog integration

7. **Oracle** - Enterprise database
   - SQLAlchemy-based connector
   - Support for Oracle-specific features (partitions, etc.)
   - Incremental loading support

8. **MongoDB** - Popular NoSQL document database
   - pymongo-based connector
   - Document collection read/write
   - Query-based incremental loading

9. **ClickHouse** - Fast analytics database
   - clickhouse-connect or clickhouse-driver connector
   - Support for ClickHouse-specific features (engines, partitions)
   - Efficient bulk inserts

10. **Elasticsearch** - Search and analytics engine
    - elasticsearch-py connector
    - Index-based read/write operations
    - Support for document indexing and querying

**Implementation Approach:**

- Leverage SQLAlchemy for SQL databases (MySQL, SQL Server, Oracle, Redshift)
- Use native SDKs for cloud platforms (Snowflake, BigQuery, Databricks)
- Follow unified connector protocol for consistency
- Support ArrowBatch for efficient data transfer
- Incremental loading will be implemented in v0.5.1 milestone
- Add comprehensive unit and integration tests

**Example Recipe:**

```yaml
source:
  type: snowflake
  account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
  user: "{{ env_var('SNOWFLAKE_USER') }}"
  password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
  warehouse: "COMPUTE_WH"
  database: "ANALYTICS"
  schema: "PUBLIC"
  table: "CUSTOMERS"
  # Incremental loading will be fully implemented in v0.5.1
  # incremental:
  #   strategy: cursor
  #   cursor_column: updated_at

destination:
  type: bigquery
  project: "{{ env_var('GCP_PROJECT') }}"
  dataset: "analytics"
  table: "customers"
  write_mode: merge
  merge_keys: [id]
```

### 12. Production Features

**Priority**: Essential for production deployments

**Goal**: Enterprise-ready observability, scheduling, and security

- [ ] **Alerting & Notifications**
  - Email/Slack alerts on failures
  - Schema change notifications
  - Data quality alerts
  - Custom alert rules

- [ ] **Pipeline Scheduling**
  - Cron-like scheduling
  - DAG scheduling (Airflow integration)
  - Event-driven triggers
  - Schedule management

- [ ] **Observability**
  - Prometheus metrics export
  - OpenTelemetry traces
  - Grafana dashboards
  - Performance profiling

- [ ] **Security**
  - Secret management integration (Vault, AWS Secrets Manager)
  - Encryption at rest
  - Audit logging
  - Role-based access control

### 11. Built-in SaaS Source Connectors

**Priority**: High value-add for common use cases

**Goal**: Provide pre-built, tested connectors for common SaaS data sources

- [ ] **REST API Source**
  - Generic REST API connector
  - Pagination support (offset, cursor, page)
  - Rate limiting
  - Authentication (API key, OAuth, JWT)
  - Incremental loading (see v0.5.1 milestone)

- [ ] **Common SaaS Sources**
  - Stripe
  - Shopify
  - Salesforce
  - Google Analytics
  - HubSpot
  - GitHub
  - Slack

- [ ] **Source Registry**
  - Community-contributed sources
  - Source testing framework
  - Source documentation generator
  - Source versioning

**Example Recipe Enhancement:**
```yaml
source:
  type: stripe
  api_key: "{{ env_var('STRIPE_API_KEY') }}"
  resource: customers
  # Incremental loading will be fully implemented in v0.5.1
  # incremental:
  #   strategy: cursor
  #   cursor_column: created
```

### 13. Rust Engine

**Priority**: Low - optimization for high-performance scenarios

**Goal**: Optional Rust acceleration for performance-critical operations

- [ ] **Arrow Batches (Rust Implementation)**
  - Native Rust Arrow implementation
  - Even better performance than Python Arrow
  - Zero-copy data transfer
  - Memory-efficient processing

- [ ] **Rust Transform DSL**
  - High-performance transform engine
  - Rust-based transform functions
  - Parallel transform execution

- [ ] **Rust I/O Readers/Writers**
  - CSV/Parquet read/write at high throughput
  - IO parallel orchestration without Python GIL
  - Bindings via PyO3

## 9. Differentiators vs Existing Tools

| Feature | This Engine | dlt-hub | Airbyte | Singer |
|---------|-------------|---------|---------|--------|
| Declarative recipes | Yes | Limited | No | No |
| Inheritance (extends:) | Yes | No | No | No |
| Python-first | Yes | Yes | No | No |
| Rust acceleration | Optional | No | No | No |
| Composable macros | Yes | No | No | No |
| Embeddable | Yes | Partial | No | No |

## 10. Repository Structure

```
dataloader/
  __init__.py             # Package exports (public API)
  api.py                  # Public Python API
  core/
    __init__.py
    batch.py              # Batch protocol and ArrowBatch
    engine.py             # Execution engine ✅
    exceptions.py         # Exception hierarchy
    state.py              # State model
    state_backend.py      # StateBackend protocol + LocalStateBackend ✅
  models/
    __init__.py
    recipe.py             # Recipe model
    source_config.py      # SourceConfig + IncrementalConfig
    destination_config.py # DestinationConfig
    transform_config.py   # TransformConfig + TransformStep
    runtime_config.py     # RuntimeConfig
    loader.py             # RecipeLoader with inheritance
    merger.py             # RecipeMerger for extends:
    templates.py          # Template rendering engine
  connectors/
    __init__.py           # Registry + exports
    base.py                 # Unified Connector protocol
    registry.py             # Unified connector registry
    postgres/
      __init__.py
      config.py             # PostgresConnectorConfig
      connector.py          # PostgresConnector (Read/Write)
    duckdb/
      __init__.py
      config.py             # DuckDBConnectorConfig
      connector.py          # DuckDBConnector (Read/Write)
    filestore/
      __init__.py
      config.py             # FileStore configs (S3, Local, etc.)
      connector.py          # FileStoreConnector (Read/Write)
      formats.py            # Format handlers (CSV, JSON, JSONL, Parquet)
  transforms/
    __init__.py           # Registry + exports
    registry.py           # Transform registry
    pipeline.py           # TransformPipeline executor
    rename.py             # rename_columns transform
    cast.py               # cast transform
    add_column.py         # add_column transform
examples/
  recipes/
    base_recipe.yaml      # Base recipe example
    customers.yaml        # Postgres → DuckDB example
    simple_csv.yaml       # CSV → S3 example
    child_recipe.yaml     # Inheritance example
tests/
  unit/                   # Unit tests
  integration/            # Integration tests
    test_recipe_loading.py
    test_end_to_end.py    # End-to-end pipeline tests ✅
pyproject.toml            # Package configuration ✅
README.md                 # User documentation ✅
ARCHITECTURE.md           # This file
```

## 11. Dependencies

```toml
[project]
dependencies = [
    "pydantic>=2.0",          # Schema validation
    "pyyaml>=6.0",            # YAML parsing
]

[project.optional-dependencies]
postgres = [
    "psycopg2-binary>=2.9",   # Postgres driver
    "sqlalchemy>=2.0.0",      # Database abstraction
]
duckdb = [
    "duckdb>=0.9",            # DuckDB database
]
s3 = [
    "boto3>=1.28",            # S3 operations
    "fsspec>=2023.1.0",       # Unified filesystem interface
    "s3fs>=2023.1.0",        # S3 backend for fsspec
]
parquet = [
    "pyarrow>=14.0",          # Parquet format support
    "pandas>=2.0",            # Data manipulation
]
sftp = [
    "paramiko>=3.0",          # SFTP support
    "fsspec>=2023.1.0",       # Unified filesystem interface
]
all = [
    "psycopg2-binary>=2.9",
    "sqlalchemy>=2.0.0",
    "duckdb>=0.9",
    "boto3>=1.28",
    "fsspec>=2023.1.0",
    "s3fs>=2023.1.0",
    "pyarrow>=14.0",
    "pandas>=2.0",
    "paramiko>=3.0",
]
dev = [
    "pytest>=7.0",
    "pytest-cov>=4.0",
    "moto>=5.0",              # AWS mocking for tests
]
```

## 12. Recipe Inheritance via `extends:` — Deep Dive

`extends:` is a central feature that allows recipes to be composed, layered, and reused, similar to configuration inheritance in Chef cookbooks or Terraform modules.

### 12.1 How It Works ✅ Implemented

```yaml
extends: base_recipe.yaml
```

Inheritance applies as:

- Load parent recipe (`base_recipe.yaml`)
- Deep merge parent → child via `RecipeMerger`
- Child overrides any field
- Child can delete inherited fields via `delete:` list
- Result is fully resolved before execution

### 12.2 Merge Rules ✅ Implemented

**Scalars → override**

```
parent.runtime.batch_size = 20000
child.runtime.batch_size = 5000
→ effective: 5000
```

**Dicts → deep merge**

Unspecified keys are inherited.

**Lists → behavior depends:**

- Transform steps → concatenated (parent first, then child)
  - Parent recipe transform steps are executed first, followed by child recipe transform steps
  - If both parent and child add columns with the same name, the second step will fail (column already exists)
  - Best practice: Use different column names in parent and child recipes, or use child recipes to override parent behavior by deleting and redefining transform steps
- Other lists → overridden

**Delete semantics** ✅ Implemented

```yaml
delete:
  - transform.steps
  - destination.merge_keys
```

Paths are dot-separated. Deletion happens after merge but before model validation.

### 12.3 Multi-level Inheritance ✅ Implemented

Fully supported with cycle detection:

```
recipe_c.yaml -->
recipe_b.yaml -->
recipe_a.yaml
```

The engine resolves deepest parent first.

### 12.4 Template Rendering ✅ Implemented

Templates are rendered after inheritance resolution:

```yaml
source:
  host: "{{ env_var('DB_HOST') }}"           # os.environ["DB_HOST"]
  database: "{{ var('database') }}"          # CLI-provided variable
  table: "{{ recipe.name }}_raw"             # Recipe metadata
```

**Template Syntax:**
- `{{ env_var('VAR_NAME') }}` - Environment variable lookup (function call)
- `{{ var('VAR_NAME') }}` - CLI-provided variable (function call, passed via `from_yaml()`)
- `{{ recipe.name }}` - Recipe metadata (dot notation)

All connection parameters are specified in recipes using templates. Templates are rendered during recipe loading, so no separate connection dictionaries are needed. Unresolved templates raise `RecipeError` with context.

## 13. Conclusion

This architecture enables a powerful, extensible, and high-performance data loading system centered on recipes, state, and clean abstractions. The ability to layer recipes (`extends:`) gives the system a unique advantage: reproducible, standardized, maintainable pipelines that work across teams and environments.

**Current Status:** 
- **v0.1 Prototype** ✅ Complete - All core components implemented
- **v0.2 Reliable MVP** ✅ Complete - Parallelism, logging, metrics, state backends, CLI

**Implemented Features:**
- Recipe model layer with inheritance and template rendering
- Unified connector architecture (PostgresConnector, DuckDBConnector, FileStoreConnector)
- FileStore connector with multiple backends (S3, local) and formats (CSV, JSON, JSONL, Parquet)
- Transform pipeline with extensible registry
- Execution engine with state management
- Public Python API (`from_yaml`, `run_recipe`, `run_recipe_from_yaml`)
- State backends (Local, S3, DynamoDB)
- Async parallelism with asyncio
- Structured logging (JSON/normal format)
- Metrics collection
- Full CLI interface
- Comprehensive documentation and example recipes
- Comprehensive test suite (219+ tests passing, including Windows compatibility tests)

**Next Steps:** 
- Complete milestone 9 (Comprehensive Integration Tests)
- Begin milestone 10 (Top 10 Datastores Connectors) - Start with MySQL, Snowflake, and BigQuery as highest priority connectors
