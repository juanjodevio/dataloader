# Changelog

All notable changes to DataLoader will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

DataLoader uses beta versioning: `0.0.0b1`, `0.0.0b2`, etc.

---

## [Unreleased]

### Added
- **LICENSE file**: Added MIT License for open source distribution
- **CONTRIBUTING.md**: Added comprehensive contribution guidelines including:
  - Code of conduct
  - Development setup instructions
  - Code style guidelines
  - Testing requirements
  - Pull request process
  - Guidelines for adding connectors and transforms
- **Version constant**: Added `__version__` to `dataloader/__init__.py` for programmatic version access
- **.editorconfig**: Added EditorConfig for consistent code formatting across editors
- **License metadata**: Added license field to `pyproject.toml`

### Fixed
- **Version consistency**: Fixed version inconsistency between `pyproject.toml` (now `0.0.0b4`) and CHANGELOG
- **FileStoreConnector S3 path handling**: Fixed multiple issues with S3FileStoreConfig path resolution
  - Fixed `_build_file_url` to correctly include path prefix when constructing S3 URLs
  - Fixed `_delete_existing_files` to properly handle S3 directory paths for overwrite mode
  - Fixed `_list_files` to correctly handle both single files and directories (no trailing slash for files)
  - Fixed path duplication when file_path already includes the path prefix
  - Resolved test failures in `test_filestore_s3_write_csv_append`, `test_full_pipeline_csv_s3`, `test_full_pipeline_json_s3`, `test_multiple_batches_append_s3`, `test_s3_overwrite_deletes_all_files`, `test_filestore_s3_read_csv_single_file`, `test_filestore_s3_read_json`, `test_filestore_s3_read_jsonl`, and `test_s3_file_url_building`

### Changed
- **README.md**: Updated to reference LICENSE and CONTRIBUTING.md files

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

