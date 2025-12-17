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
     ┌────────────────────────┐
     │     Execution Engine   │
     ├─────────┬──────────────┤
     │ State   │ Transform     │
     │ Mgmt    │ Pipeline      │
     └─────┬───┴───────┬──────┘
           │           │
           ▼           ▼
   ┌─────────────┐  ┌──────────────┐
   │ Source Plug- │  │ Destination   │
   │     ins      │  │    Plug-ins   │
   └──────┬───────┘  └───────┬───────┘
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
"{{ env.DB_HOST }}"      # Environment variables
"{{ var.table_name }}"   # CLI-provided variables  
"{{ recipe.name }}"      # Recipe metadata
```

### 5.2 Connectors – Plug-in Architecture ✅ Implemented

Two key protocols defined in `connectors/base.py`:

```python
@runtime_checkable
class Source(Protocol):
    def read_batches(self, state: State) -> Iterable[Batch]: ...

@runtime_checkable
class Destination(Protocol):
    def write_batch(self, batch: Batch, state: State) -> None: ...
```

**Registry Pattern** (`connectors/registry.py`):

```python
# Registration via decorator (preferred)
@register_source("postgres")
def create_postgres_source(config, connection):
    return PostgresSource(config, connection)

# Or direct call
register_source("csv", create_csv_source)

# Retrieval
source = get_source("postgres", config, connection)
```

**Implemented Source Connectors:**

| Connector | Stack | Features |
|-----------|-------|----------|
| `PostgresSource` | SQLAlchemy + psycopg2 | Streaming results, cursor-based incremental, schema introspection |
| `CSVSource` | stdlib csv | Batched reading, type inference, header detection |
| `S3Source` | boto3 + fsspec | Object discovery via boto3, reads via fsspec, incremental by modification time |

**Technology Choices:**

- **Databases (Postgres, RDS):** SQLAlchemy for dialect abstraction
  - Supports Postgres, MySQL, Redshift via dialect parameter
  - Connection pooling, streaming results
  - Schema introspection via `inspect()`

- **S3 and Filesystem:**
  - **boto3** for discovery + metadata (fast, explicit, cheap)
  - **fsspec** for reads/writes (clean file-like interface)

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

### 5.4 State Management ✅ Protocol Defined

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
    def load(self, recipe_name: str) -> State: ...
    def save(self, recipe_name: str, state: State) -> None: ...
```

Planned backends:
- Local JSON ✅ Implemented
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

**DictBatch Implementation:**

```python
batch = DictBatch(
    columns=["id", "name", "updated_at"],
    rows=[[1, "Alice", "2024-01-01"], [2, "Bob", "2024-01-02"]],
    metadata={"source_type": "postgres", "table": "users"}
)
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

### 5.7 Execution Engine 🚧 Planned

Core loop:

```python
for batch in source.read_batches(state):
    batch = transformer.apply(batch)
    destination.write_batch(batch, state)
    state_backend.save(recipe.name, state)
```

## 6. Rust Engine (Optional) 🔮 Future

Rust is introduced for:

- Arrow-formatted batching
- Polars-based transforms
- CSV/Parquet read/write at high throughput
- IO parallel orchestration without Python GIL
- Bindings via PyO3

## 7. Developer Experience

### 7.1 Python API

```python
from dataloader.models import Recipe
from dataloader.models.loader import RecipeLoader

# Load recipe with inheritance and templates
loader = RecipeLoader()
recipe = loader.load("recipes/customers.yaml", cli_vars={"env": "prod"})

# Get source connector
from dataloader.connectors import get_source
source = get_source(recipe.source.type, recipe.source, connection_dict)

# Read batches
for batch in source.read_batches(state):
    print(f"Read {batch.row_count} rows")
```

### 7.2 CLI 🚧 Planned

```bash
dataloader init
dataloader run recipe.yaml
dataloader validate recipe.yaml
dataloader show-state <recipe>
dataloader list-connectors
```

## 8. Roadmap

### v0.1 – Prototype 🔄 In Progress

- [x] Recipe model layer (Pydantic)
- [x] Recipe inheritance (`extends:`)
- [x] Template rendering (`{{ env.* }}`, `{{ var.* }}`)
- [x] Delete semantics for inheritance
- [x] Source/Destination protocols
- [x] Connector registry (decorator pattern)
- [x] PostgresSource (SQLAlchemy)
- [x] CSVSource
- [x] S3Source (boto3 + fsspec)
- [x] Batch and State models
- [x] Exception hierarchy
- [x] Transform pipeline executor
- [x] Transform registry (decorator pattern)
- [x] Basic transforms (rename_columns, cast, add_column)
- [ ] DuckDB destination
- [ ] Execution engine
- [ ] Local JSON state backend

### v0.2 – Reliable MVP

- Parallelism
- Retry logic
- Better logging/metrics
- S3/DynamoDB state
- CLI

### v0.3 – Rust Engine

- Arrow Batches
- Rust transform DSL
- Rust I/O readers/writers

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
  core/
    __init__.py
    batch.py              # Batch protocol and DictBatch
    engine.py             # Execution engine (planned)
    exceptions.py         # Exception hierarchy
    state.py              # State model
    state_backend.py      # StateBackend protocol
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
    base.py               # Source/Destination protocols
    registry.py           # Connector registry
    postgres/
      __init__.py
      source.py           # PostgresSource (SQLAlchemy)
    csv/
      __init__.py
      source.py           # CSVSource
    s3/
      __init__.py
      source.py           # S3Source (boto3 + fsspec)
    duckdb/               # (planned)
  transforms/
    __init__.py           # Registry + exports
    registry.py           # Transform registry
    pipeline.py           # TransformPipeline executor
    rename.py             # rename_columns transform
    cast.py               # cast transform
    add_column.py         # add_column transform
examples/
  recipes/
    base_recipe.yaml
    child_recipe.yaml
tests/
  unit/
  integration/
pyproject.toml
README.md
ARCHITECTURE.md
```

## 11. Dependencies

```toml
[project]
dependencies = [
    "pydantic>=2.0.0",        # Schema validation
    "pyyaml>=6.0.0",          # YAML parsing
    "sqlalchemy>=2.0.0",      # Database abstraction
    "psycopg2-binary>=2.9.0", # Postgres driver
    "boto3>=1.26.0",          # S3 discovery/metadata
    "fsspec>=2023.1.0",       # Unified filesystem interface
    "s3fs>=2023.1.0",         # S3 backend for fsspec
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
  host: "{{ env.DB_HOST }}"           # os.environ["DB_HOST"]
  database: "{{ var.database }}"      # CLI-provided variable
  table: "{{ recipe.name }}_raw"      # Recipe metadata
```

Unresolved templates raise `RecipeError` with context.

## 13. Conclusion

This architecture enables a powerful, extensible, and high-performance data loading system centered on recipes, state, and clean abstractions. The ability to layer recipes (`extends:`) gives the system a unique advantage: reproducible, standardized, maintainable pipelines that work across teams and environments.

**Current Status:** v0.1 prototype in progress with recipe layer, source connectors, transform pipeline, and core infrastructure complete.
