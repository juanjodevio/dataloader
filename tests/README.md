# Dataloader Tests

This directory contains unit and integration tests for the dataloader package.

## Structure

```
tests/
├── conftest.py              # Shared pytest fixtures
├── unit/                    # Unit tests
│   ├── test_source_config.py
│   ├── test_destination_config.py
│   ├── test_transform_config.py
│   ├── test_runtime_config.py
│   ├── test_recipe.py
│   ├── test_templates.py
│   ├── test_merger.py
│   └── test_loader.py
├── integration/            # Integration tests
│   └── test_recipe_loading.py
└── fixtures/               # Test fixtures and data files
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

- ✅ Pydantic model validation (SourceConfig, DestinationConfig, TransformConfig, RuntimeConfig, Recipe)
- ✅ Template rendering (env_var, var, recipe.name)
- ✅ Inheritance resolution (single, multi-level, cycles)
- ✅ Deep merge algorithm (scalars, dicts, lists, transform steps)
- ✅ Delete semantics
- ✅ Recipe loading from YAML files
- ✅ Error handling (file not found, invalid YAML, validation errors)

## Fixtures

Shared fixtures are defined in `conftest.py`:

- `temp_dir`: Temporary directory for test files
- `recipe_dir`: Temporary recipes subdirectory
- `env_vars`: Environment variables for testing
- `cli_vars`: CLI variables for testing

## Adding New Tests

When adding new tests:

1. Place unit tests in `tests/unit/`
2. Place integration tests in `tests/integration/`
3. Use descriptive test class and method names
4. Follow the existing test structure and patterns
5. Use fixtures from `conftest.py` when appropriate
6. Mark integration tests with `@pytest.mark.integration`

