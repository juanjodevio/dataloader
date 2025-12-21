# Changelog

All notable changes to DataLoader will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

DataLoader uses beta versioning: `0.0.0b1`, `0.0.0b2`, etc.

---

## [Unreleased]

### In Progress
- Milestone 9: Comprehensive Integration Tests
- Milestone 10: Top 10 Datastores Connectors

---

## [0.0.0b4] - 2024-12-21

### Milestone 4: Arrow Batch Support ✅

#### Added
- `ArrowBatch` class conforming to `Batch` protocol
- PyArrow for Arrow format support
- Zero-copy data transfer between connectors
- Memory-efficient processing for large datasets

#### Changed
- All connectors now use `ArrowBatch` exclusively
- PostgresConnector: Arrow-based reads/writes using pandas + Arrow
- DuckDBConnector: Native Arrow support with zero-copy operations
- FileStoreConnector: All formats return/accept ArrowBatch
- Removed `DictBatch` completely from codebase

#### Performance
- Reduced memory footprint
- Faster data transfer between connectors
- Better integration with Arrow-based tools (Polars, DuckDB)
- Efficient Arrow-native operations in transforms

---

## [0.0.0b3] - 2024-12-21

### Milestone 3: Optional Dependencies ✅

#### Added
- Optional dependencies (extras) for modular installation
- `[postgres]` extra: psycopg2-binary, sqlalchemy, pandas
- `[s3]` extra: boto3, s3fs
- `[duckdb]` extra: duckdb
- `[parquet]` extra: pandas
- `[all]` extra: all optional dependencies
- Clear `ImportError` messages for missing optional dependencies

#### Changed
- Made `pyarrow` and `fsspec` mandatory core dependencies
- Split dependencies into optional extras based on connector needs
- Updated `pyproject.toml` with extras configuration
- Updated installation documentation with examples

#### Installation Examples
```bash
# Minimal installation
pip install dataloader

# With specific connectors
pip install dataloader[postgres,duckdb]
pip install dataloader[s3,parquet]

# All connectors
pip install dataloader[all]
```

---

## [0.0.0b2] - 2024-XX-XX

### Milestone 2: Reliable MVP ✅

#### Added
- Parallelism with asyncio-based batch processing
- Structured logging (JSON and normal formats)
- Metrics collection (batches, rows, errors, performance)
- S3 state backend for distributed state storage
- DynamoDB state backend for distributed state storage
- Full CLI interface with all commands
- State backend factory for easy backend creation

#### CLI Commands
- `dataloader run` - Execute recipes
- `dataloader validate` - Validate recipe syntax
- `dataloader show-state` - Display recipe state
- `dataloader init` - Initialize new recipe projects
- `dataloader list-connectors` - List available connectors
- `dataloader test-connection` - Test connections
- `dataloader dry-run` - Simulate execution
- `dataloader resume` - Resume failed executions
- `dataloader cancel` - Cancel running recipes

---

## [0.0.0b1] - 2024-XX-XX

### Milestone 1: Prototype ✅

#### Added
- Recipe model layer with Pydantic validation
- Recipe inheritance via `extends:` keyword
- Template rendering (`{{ env_var() }}`, `{{ var() }}`, `{{ recipe.name }}`)
- Delete semantics for inheritance
- Unified Connector protocol (replaces separate Source/Destination)
- Unified connector registry with decorator pattern
- **PostgresConnector** - Read/Write support with SQLAlchemy
- **DuckDBConnector** - Read/Write support for file-based and in-memory databases
- **FileStoreConnector** - Unified file storage with multiple backends and formats
  - S3 backend support
  - Local filesystem backend
  - CSV, JSON, JSONL, Parquet formats
  - Extensible format registry
- Batch and State models
- Exception hierarchy (`DataLoaderError` and subclasses)
- Transform pipeline executor
- Transform registry with decorator pattern
- Basic transforms: `rename_columns`, `cast`, `add_column`
- Execution engine for batch processing
- Local JSON state backend (`LocalStateBackend`)
- Public Python API: `from_yaml()`, `run_recipe()`, `run_recipe_from_yaml()`
- Example recipes in `examples/recipes/`
- Comprehensive documentation (README.md, ARCHITECTURE.md)
- Basic integration tests
- Comprehensive test suite (219+ tests passing, including Windows compatibility)

#### Core Features
- Declarative YAML-based recipes
- Incremental loading with state persistence
- Transform pipelines with extensible transforms
- Multi-format file support (CSV, JSON, JSONL, Parquet)
- Cross-platform compatibility (Windows, Linux, macOS)

---

## Contributing

See [ARCHITECTURE.md](ARCHITECTURE.md) for detailed architecture and roadmap.

