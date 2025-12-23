# Dataloader Tests

This directory contains unit and integration tests for the dataloader package.

## Structure

```
tests/
├── conftest.py                           # Shared pytest fixtures
├── unit/                                 # Unit tests
│   ├── test_api_connector.py            # API connector unit tests
│   ├── test_arrow_batch.py              # ArrowBatch implementation tests
│   ├── test_connectors.py               # Connector protocol/interface tests
│   ├── test_destination_config.py       # Destination configuration validation
│   ├── test_duckdb_connector.py         # DuckDB connector (including full refresh)
│   ├── test_filestore_local_connector.py # Local file system connector
│   ├── test_filestore_s3_connector.py   # S3 connector tests
│   ├── test_loader.py                   # Recipe loading and custom transforms
│   ├── test_merger.py                   # Recipe inheritance and merging
│   ├── test_metrics.py                  # Pipeline metrics tracking
│   ├── test_parallel.py                 # Parallel execution tests
│   ├── test_postgres_connector.py       # PostgreSQL connector (including full refresh)
│   ├── test_recipe.py                   # Recipe model validation
│   ├── test_runtime_config.py           # Runtime configuration (batch_size, full_refresh, custom_transforms)
│   ├── test_schema_config.py            # Schema configuration
│   ├── test_schema_registry.py          # Schema registry
│   ├── test_schema.py                   # Schema validation
│   ├── test_source_config.py            # Source configuration validation
│   ├── test_state_backends.py           # State persistence backends
│   ├── test_templates.py                # Template rendering (env_var, var, recipe.name)
│   ├── test_transform_config.py         # Transform configuration
│   ├── test_transforms.py               # Built-in transforms (rename, cast, add_column)
│   └── test_type_mappers.py             # Type mapping utilities
├── integration/                          # Integration tests
│   ├── test_end_to_end.py               # End-to-end pipeline tests (CSV→DuckDB, API→FileStore, etc.)
│   └── test_recipe_loading.py           # Recipe loading and inheritance integration tests
└── fixtures/                             # Test fixtures and data files
```

## Running Tests

### Run all tests
```bash
uv run pytest
```

### Run only unit tests
```bash
uv run pytest tests/unit/
```

### Run only integration tests
```bash
uv run pytest tests/integration/
```

### Run with coverage
```bash
uv run pytest --cov=dataloader --cov-report=html
```

### Run specific test file
```bash
uv run pytest tests/unit/test_source_config.py
```

### Run specific test
```bash
uv run pytest tests/unit/test_source_config.py::TestSourceConfig::test_postgres_source_valid
```

## Test Coverage

Current test coverage includes:

### Configuration & Models
- ✅ Pydantic model validation (SourceConfig, DestinationConfig, TransformConfig, RuntimeConfig, Recipe)
- ✅ Template rendering (env_var, var, recipe.name)
- ✅ Schema configuration and validation
- ✅ Runtime configuration (batch_size, parallelism, full_refresh, custom_transforms)

### Recipe System
- ✅ Recipe loading from YAML files
- ✅ Inheritance resolution (single, multi-level, cycles)
- ✅ Deep merge algorithm (scalars, dicts, lists, transform steps)
- ✅ Delete semantics
- ✅ Error handling (file not found, invalid YAML, validation errors)

### Connectors

#### Source Connectors
- ✅ **API Connector**: REST API data extraction, JSONPath support (`data_path`), pagination, authentication
- ✅ **FileStore Connector**: Local and S3 backends, multiple formats (CSV, JSON, JSONL, Parquet), glob patterns
- ✅ **PostgreSQL Connector**: Query execution, schema evolution, incremental loading
- ✅ **DuckDB Connector**: Query execution, schema evolution

#### Destination Connectors
- ✅ **PostgreSQL Connector**: Write modes (append, overwrite), full refresh (DROP TABLE), schema evolution, batch inserts
- ✅ **DuckDB Connector**: Write modes (append, overwrite), full refresh (DROP TABLE), batch inserts
- ✅ **FileStore Connector**: Write modes (append, overwrite), full refresh (recursive path deletion), multiple formats

### Transforms
- ✅ Built-in transforms: `rename_columns`, `cast`, `add_column`
- ✅ Transform pipeline execution (sequential, parallel)
- ✅ **Custom Transforms**: Loading from modules/files, registration via `@register_transform`, `BaseTransform` protocol
- ✅ Transform registry and factory pattern

### Runtime Features
- ✅ Batch processing and size configuration
- ✅ Parallel execution (multi-worker)
- ✅ **Full Refresh**: Destructive operations (DROP TABLE, path deletion) for all destinations
- ✅ State management and persistence (LocalStateBackend)
- ✅ Incremental loading (cursor-based strategy)
- ✅ Metrics tracking (execution time, rows processed, errors)

### Integration Tests
- ✅ End-to-end pipelines: CSV → DuckDB, CSV → PostgreSQL, API → FileStore, API → DuckDB
- ✅ Transform application in pipelines (renaming, casting, adding columns)
- ✅ Recipe inheritance in execution
- ✅ Incremental loading workflows
- ✅ State persistence between runs

## Fixtures

Shared fixtures are defined in `conftest.py`:

- `temp_dir`: Temporary directory for test files
- `recipe_dir`: Temporary recipes subdirectory
- `env_vars`: Environment variables for testing
- `cli_vars`: CLI variables for testing

## Key Testing Patterns

### Custom Transforms
Custom transforms must:
1. Inherit from `BaseTransform` (a `@runtime_checkable` Protocol)
2. Implement the `apply(self, batch: Batch) -> Batch` method
3. Be registered using `@register_transform("transform_name")` decorator or `register_transform()` function

Example test pattern:
```python
from dataloader.transforms.registry import BaseTransform, register_transform

class MyCustomTransform(BaseTransform):
    def apply(self, batch: Batch) -> Batch:
        # Transform logic
        return transformed_batch

@register_transform("my_custom_transform")
def create_my_transform(config: dict[str, Any]) -> MyCustomTransform:
    return MyCustomTransform(config)
```

### Full Refresh Testing
When testing full refresh (destructive operations):
- For databases: Verify `DROP TABLE IF EXISTS` is executed before table creation
- For FileStore: Verify entire path is recursively deleted before writing
- Test both `overwrite` and `append` write modes with `full_refresh=True`
- Ensure operations only happen on first batch (use `_batch_counter == 0` check)

### Connector Testing
Connector tests should cover:
- Read operations (if source connector)
- Write operations (if destination connector)
- Error handling (connection failures, invalid configurations)
- Schema evolution (adding columns)
- Full refresh behavior
- Resource cleanup (closing connections, releasing file handles)

## Example Usage

The `examples/open_meteo/` directory demonstrates several key features:

### Features Demonstrated
- **API Connector**: Fetching data from Open Meteo REST API with `data_path` for JSONPath extraction
- **Custom Transforms**: Custom transform class (`FlattenOpenMeteoDailyTransform`) registered via `@register_transform`
- **Custom Transform Loading**: Loading custom transforms from recipe via `runtime.custom_transforms`
- **Recipe Inheritance**: Base recipe (`base_open_meteo.yml`) with shared configuration
- **Transform Pipeline**: Multiple transform steps (add_column, custom flatten transform)
- **Multiple Destinations**: Same source/transform with different destinations (DuckDB, FileStore)

### Example Structure
```
examples/open_meteo/
├── flatten_data.py           # Custom transform implementation
├── main.py                   # Entry point for running recipes
├── recipes/
│   ├── base_open_meteo.yml   # Base recipe with shared config
│   ├── api_to_duckdb.yml     # API → DuckDB pipeline
│   └── api_to_local.yml      # API → Local FileStore pipeline
└── README.md                 # Example documentation
```

### Running the Example
```bash
# Run API to DuckDB
python -m examples.open_meteo.main --recipe api_to_duckdb

# Run API to Local FileStore
python -m examples.open_meteo.main --recipe api_to_local
```

## Adding New Tests

When adding new tests:

1. Place unit tests in `tests/unit/`
2. Place integration tests in `tests/integration/`
3. Use descriptive test class and method names
4. Follow the existing test structure and patterns
5. Use fixtures from `conftest.py` when appropriate
6. Mark integration tests with `@pytest.mark.integration`
7. For custom transforms, test both the transform class and its registration
8. For connectors, test both read and write operations, including full refresh when applicable

