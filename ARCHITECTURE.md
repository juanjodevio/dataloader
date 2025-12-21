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

**DictBatch Implementation** (Current):

```python
batch = DictBatch(
    columns=["id", "name", "updated_at"],
    rows=[[1, "Alice", "2024-01-01"], [2, "Bob", "2024-01-02"]],
    metadata={"source_type": "postgres", "table": "users"}
)
```

**ArrowBatch Implementation** (Planned):

Future implementation will support Apache Arrow format for:
- Zero-copy data transfer
- Memory-efficient processing
- Better performance for large datasets
- Native integration with Arrow-based tools (Polars, DuckDB, etc.)

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

## 6. Rust Engine (Optional) 🔮 Future

Rust is introduced for:

- Arrow-formatted batching
- Polars-based transforms
- CSV/Parquet read/write at high throughput
- IO parallel orchestration without Python GIL
- Bindings via PyO3

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
- Core classes: `Recipe`, `State`, `StateBackend`, `LocalStateBackend`, `Batch`, `DictBatch`
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

### v0.1 – Prototype ✅ Complete

- [x] Recipe model layer (Pydantic)
- [x] Recipe inheritance (`extends:`)
- [x] Template rendering (`{{ env_var('VAR') }}`, `{{ var('VAR') }}`, `{{ recipe.name }}`)
- [x] Delete semantics for inheritance
- [x] Unified Connector protocol (replaces separate Source/Destination)
- [x] Unified connector registry (decorator pattern)
- [x] PostgresConnector (SQLAlchemy) - Read/Write
- [x] DuckDBConnector - Read/Write
- [x] FileStoreConnector (fsspec + format handlers) - Read/Write
  - [x] S3 backend
  - [x] Local filesystem backend
  - [x] CSV, JSON, JSONL, Parquet formats
  - [x] Extensible format registry
- [x] Batch and State models
- [x] Exception hierarchy
- [x] Transform pipeline executor
- [x] Transform registry (decorator pattern)
- [x] Basic transforms (rename_columns, cast, add_column)
- [x] Execution engine ✅
- [x] Local JSON state backend (`LocalStateBackend`) ✅
- [x] Public Python API (`from_yaml`, `run_recipe`, `run_recipe_from_yaml`) ✅
- [x] Example recipes ✅
- [x] Documentation (README.md) ✅
- [x] Integration tests ✅
- [x] Comprehensive test suite (219+ tests passing, including Windows compatibility tests)

### v0.2 – Reliable MVP ✅ Complete

- [x] Parallelism (asyncio-based)
- [x] Structured logging (JSON/normal format)
- [x] Metrics collection
- [x] S3/DynamoDB state backends
- [x] Full CLI interface
- [x] State backend factory

### v0.2.1 – Optional Dependencies & Integration Tests 🚧 In Progress

**Goal**: Improve dependency management and test coverage

- [ ] **Optional Dependencies (Extras)**
  - Split dependencies into optional extras based on connector needs
  - `[postgres]` extra: psycopg2-binary, sqlalchemy
  - `[s3]` extra: boto3, s3fs, fsspec
  - `[duckdb]` extra: duckdb
  - `[parquet]` extra: pyarrow, pandas
  - `[all]` extra: all optional dependencies
  - Update `pyproject.toml` with extras configuration
  - Update installation docs with examples

- [ ] **Enhanced Integration Tests**
  - Expand integration test coverage
  - Test multi-connector pipelines (Postgres → FileStore → DuckDB)
  - Test format conversions (CSV → JSON → Parquet)
  - Test incremental loading across different connectors
  - Test error recovery and state persistence
  - Test recipe inheritance with connectors
  - Add performance benchmarks

**Example Installation:**

```bash
# Minimal installation
pip install dataloader

# With specific connectors
pip install dataloader[postgres,duckdb]
pip install dataloader[s3,parquet]

# All connectors
pip install dataloader[all]
```

### v0.3 – Schema Management & Type System

**Goal**: Automatic schema inference, evolution, and rich type handling

- [ ] **Schema Inference & Evolution**
  - Automatic schema detection from source data
  - Schema versioning (track schema changes over time)
  - Schema migration support
  - Schema registry (store schemas separately from state)
  - Add `SchemaManager` class

- [ ] **Data Type System**
  - Rich type system (string, int, float, date, datetime, json, array, struct)
  - Automatic type inference from data
  - Type coercion and validation
  - Support for complex/nested types
  - Add `TypeInferrer` class

- [ ] **Schema Configuration**
  - Allow schema override in recipes
  - Define expected schemas in YAML
  - Validate incoming data against schema
  - Schema enforcement modes (strict, lenient, infer)

**Example Recipe Enhancement:**
```yaml
name: customers_pipeline

source:
  type: postgres
  table: customers

schema:
  mode: strict  # or: infer, lenient
  columns:
    - name: id
      type: integer
      nullable: false
      primary_key: true
    - name: email
      type: string
      nullable: false
      unique: true
    - name: created_at
      type: datetime
      nullable: false
  evolution:
    allow_new_columns: true
    allow_column_deletion: false
    allow_type_changes: false

destination:
  type: duckdb
  database: output.duckdb
  table: customers
```

### v0.4 – Data Normalization

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

### v0.8 – Built-in Verified Sources

**Goal**: Provide pre-built, tested connectors for common data sources

- [ ] **REST API Source**
  - Generic REST API connector
  - Pagination support (offset, cursor, page)
  - Rate limiting
  - Authentication (API key, OAuth, JWT)
  - Incremental loading

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
  incremental:
    strategy: cursor
    cursor_column: created
```

### v0.9 – Production Features

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

### v0.3.1 – Arrow Batch Support

**Goal**: Add Apache Arrow batch format for improved performance

- [ ] **ArrowBatch Implementation**
  - Implement `ArrowBatch` class conforming to `Batch` protocol
  - Use PyArrow for Arrow format support
  - Zero-copy data transfer between connectors
  - Memory-efficient processing for large datasets

- [ ] **Connector Arrow Support**
  - Update connectors to support Arrow batches
  - PostgresConnector: Arrow-based reads/writes
  - DuckDBConnector: Native Arrow support
  - FileStoreConnector: Arrow format handler
  - Automatic conversion between DictBatch and ArrowBatch

- [ ] **Performance Benefits**
  - Reduced memory footprint
  - Faster data transfer
  - Better integration with Arrow-based tools (Polars, DuckDB)
  - Parallel processing without GIL limitations

**Example Usage:**

```python
from dataloader import ArrowBatch

# Connectors can return ArrowBatch for better performance
for batch in connector.read_batches(state):
    # batch is ArrowBatch, zero-copy operations
    transformed = transform.apply(batch)
    destination.write_batch(transformed, state)
```

### v0.10 – Rust Engine (Performance)

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

### v0.11 – SFTP Backend Support

**Goal**: Add SFTP backend to FileStore connector for secure file transfer

- [ ] **SFTP Backend Implementation**
  - Add SFTP backend to FileStore connector
  - Support SFTP authentication (password, key-based)
  - Implement SFTP file operations (read, write, list, delete)
  - Add `SFTPFileStoreConfig` configuration class
  - Update backend detection logic
  - Add SFTP integration tests

**Example Recipe with SFTP:**

```yaml
source:
  type: filestore
  backend: sftp
  host: sftp.example.com
  username: "{{ env_var('SFTP_USER') }}"
  password: "{{ env_var('SFTP_PASSWORD') }}"
  path: /data/export/
  format: csv
```

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
    batch.py              # Batch protocol and DictBatch
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
- v0.2.1 (Optional Dependencies & Integration Tests) - See roadmap above
- v0.3 (Schema Management & Type System) - See roadmap above for detailed feature plans through v0.10
