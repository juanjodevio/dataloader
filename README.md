# DataLoader

Recipe-driven data loading framework for declarative EL (Extract-Load) workflows.

DataLoader enables you to define data pipelines using simple YAML recipes, handling reliability, batching, retries, and state management automatically. Define what you want to sync, not how.

## Features

- **Declarative Recipes**: Define data pipelines in YAML with inheritance support
- **Incremental Loads**: Automatic cursor-based incremental loading with state persistence
- **Transform Pipeline**: Built-in transforms (rename, cast, add columns) with extensible architecture
- **Multiple Connectors**: Support for Postgres, DuckDB, S3, CSV, and more
- **State Management**: Persistent state for resumable and incremental loads (Local, S3, DynamoDB)
- **Template System**: Environment variables and recipe metadata in configurations
- **Parallel Execution**: Async batch processing with configurable parallelism
- **Structured Logging**: JSON or normal format logs with context
- **Metrics Collection**: Track batches, rows, errors, and performance
- **CLI Interface**: Full command-line interface for all operations

## Installation

### Installation

Install the core package (includes pyarrow and fsspec for core functionality):

```bash
pip install dataloader
```

### With Optional Dependencies

Install with specific connector support:

```bash
# PostgreSQL connector
pip install dataloader[postgres]

# S3 connector
pip install dataloader[s3]

# DuckDB connector
pip install dataloader[duckdb]

# Parquet format support
pip install dataloader[parquet]

# Multiple connectors
pip install dataloader[postgres,duckdb]
pip install dataloader[s3,parquet]

# All connectors and formats
pip install dataloader[all]
```

### Development Installation

For development with all dependencies and test tools:

```bash
pip install -e ".[all,dev]"
```

Or install dependencies separately:

```bash
pip install -e .
pip install -r requirements.txt  # if available
```

## Quick Start

### 1. Install Dependencies

```bash
pip install -e .
```

### 2. Create an Example Recipe

Create a file `my_recipe.yaml`:

```yaml
name: load_customers

source:
  type: postgres
  host: localhost
  database: mydb
  user: postgres
  password: postgres
  table: public.customers

transform:
  steps:
    - type: rename_columns
      mapping:
        fname: first_name
        lname: last_name

destination:
  type: duckdb
  database: customers.duckdb
  table: customers
  write_mode: append
```

### 3. Run the Recipe

```python
from dataloader import run_recipe_from_yaml

run_recipe_from_yaml("my_recipe.yaml")
```

## API Documentation

### Loading Recipes

#### `from_yaml(path: str) -> Recipe`

Load a recipe from a YAML file with inheritance resolution.

```python
from dataloader import from_yaml

recipe = from_yaml("examples/recipes/customers.yaml")
print(recipe.name)  # "customers"
```

**Parameters:**
- `path`: Path to recipe YAML file

**Returns:**
- `Recipe`: Fully resolved Recipe instance

**Raises:**
- `RecipeError`: If file not found, invalid YAML, validation fails, or cycle detected

### Executing Recipes

#### `run_recipe(recipe: Recipe, state_backend: StateBackend) -> None`

Execute a recipe with given state backend.

```python
from dataloader import from_yaml, run_recipe, LocalStateBackend

recipe = from_yaml("examples/recipes/customers.yaml")
state_backend = LocalStateBackend(".state")

run_recipe(recipe, state_backend)
```

**Parameters:**
- `recipe`: Recipe instance to execute. Connection parameters are specified
            in the recipe and rendered during loading via templates.
- `state_backend`: Backend for loading and saving state

**Raises:**
- `EngineError`: If execution fails at any step
- `ConnectorError`: If connector creation fails
- `TransformError`: If transform execution fails
- `StateError`: If state operations fail

#### `run_recipe_from_yaml(recipe_path: str, state_dir: str = ".state") -> None`

Convenience function that combines `from_yaml()` and `run_recipe()`.

```python
from dataloader import run_recipe_from_yaml

run_recipe_from_yaml("examples/recipes/customers.yaml", state_dir=".state")
```

**Parameters:**
- `recipe_path`: Path to recipe YAML file
- `state_dir`: Directory to store state files (default: ".state")

## Connection Configuration

All connection parameters are specified directly in the recipe YAML file using Jinja2-style templates. Templates are rendered during recipe loading, allowing you to inject values from environment variables or CLI variables.

### Template Syntax

Recipes support the following template patterns:

- `{{ env_var('VAR_NAME') }}` - Environment variable lookup
- `{{ var('VAR_NAME') }}` - CLI-provided variable (passed via `from_yaml()`)
- `{{ recipe.name }}` - Recipe metadata

### Example

```yaml
source:
  type: postgres
  host: "{{ env_var('DB_HOST') }}"
  database: "{{ env_var('DB_NAME') }}"
  user: "{{ env_var('DB_USER') }}"
  password: "{{ env_var('DB_PASSWORD') }}"
  table: public.customers
```

Connection parameters are specified in the recipe and rendered during loading, so there's no need to pass connections separately to the API.

## Example Recipes

### Base Recipe

The base recipe (`examples/recipes/base_recipe.yaml`) provides common configuration:

```yaml
name: base_recipe

runtime:
  batch_size: 10000

transform:
  steps:
    - type: add_column
      name: _loaded_at
      value: "{{ recipe.name }}"
```

### Customers Recipe

Example using recipe inheritance (`examples/recipes/customers.yaml`):

```yaml
name: customers

extends: base_recipe.yaml

source:
  type: postgres
  host: "{{ env.DB_HOST | default('localhost') }}"
  database: "{{ env.DB_NAME | default('testdb') }}"
  user: "{{ env.DB_USER | default('postgres') }}"
  password: "{{ env.DB_PASSWORD | default('postgres') }}"
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
    - type: add_column
      name: _loaded_at
      value: "{{ recipe.name }}"

destination:
  type: duckdb
  database: "{{ env.DUCKDB_PATH | default('customers.duckdb') }}"
  table: customers
  write_mode: append
```

### Simple CSV Recipe

Minimal example for quick start (`examples/recipes/simple_csv.yaml`):

```yaml
name: simple_csv

source:
  type: csv
  path: data/input.csv

transform:
  steps: []

destination:
  type: s3
  bucket: my-bucket
  path: output/data.csv
  region: us-east-1
  write_mode: overwrite

runtime:
  batch_size: 5000
```

## Architecture Overview

For detailed architecture information, see [ARCHITECTURE.md](ARCHITECTURE.md).

Key components:

- **Recipe Model Layer**: YAML parsing, validation, inheritance resolution, template rendering
- **Connector System**: Pluggable source and destination connectors
- **Transform Pipeline**: Sequential transform steps with extensible registry
- **State Management**: Persistent state for incremental loads
- **Execution Engine**: Core loop for batch processing

## Supported Connectors

### Sources
- **Postgres**: PostgreSQL databases with SQLAlchemy (requires `[postgres]` extra)
- **FileStore**: Local filesystem or S3 files (CSV, JSON, JSONL, Parquet)
  - Local filesystem: uses fsspec (included in core)
  - S3 backend: requires `[s3]` extra (boto3, s3fs)
  - Parquet format: requires `[parquet]` extra (pandas)

### Destinations
- **DuckDB**: DuckDB databases (file-based or in-memory) (requires `[duckdb]` extra)
- **FileStore**: Local filesystem or S3 files with various formats
  - Parquet format: requires `[parquet]` extra (pandas)

### Optional Dependencies

DataLoader uses optional dependencies for connector-specific functionality. Core dependencies (pyarrow, fsspec) are always included:

- **`[postgres]`**: PostgreSQL connector (psycopg2-binary, sqlalchemy, pandas)
- **`[s3]`**: S3 connector (boto3, s3fs)
- **`[duckdb]`**: DuckDB connector (duckdb)
- **`[parquet]`**: Parquet format support (pandas)
- **`[all]`**: All optional dependencies
- **`[dev]`**: Development dependencies (pytest, pytest-cov, moto)

**Note**: The core package includes essential dependencies (pydantic, pyyaml, click, json-log-formatter, pyarrow, fsspec). Connectors will raise clear `ImportError` messages if required optional dependencies are missing.

## Transform Types

- **rename_columns**: Rename columns using a mapping
- **cast**: Cast columns to specified types
- **add_column**: Add a new column with a constant or template value

## Template System

Recipes support template variables:

- `{{ env.VAR_NAME }}`: Environment variables
- `{{ var.NAME }}`: CLI-provided variables (future)
- `{{ recipe.name }}`: Recipe metadata

Example:

```yaml
host: "{{ env.DB_HOST }}"
table: "{{ var.table_name }}"
value: "{{ recipe.name }}"
```

## CLI Interface

DataLoader provides a full command-line interface for all operations:

### Core Commands

- **`dataloader run`**: Execute a recipe
  ```bash
  dataloader run recipe.yaml
  dataloader run recipe.yaml --state-backend s3://bucket/state
  dataloader run recipe.yaml --vars table=customers --log-level DEBUG
  ```

- **`dataloader validate`**: Validate a recipe
  ```bash
  dataloader validate recipe.yaml
  ```

- **`dataloader show-state`**: Display recipe state
  ```bash
  dataloader show-state my_recipe
  dataloader show-state my_recipe --json
  ```

- **`dataloader init`**: Initialize a new recipe project
  ```bash
  dataloader init
  dataloader init --recipe-name customers
  ```

- **`dataloader list-connectors`**: List available connectors
  ```bash
  dataloader list-connectors
  ```

- **`dataloader test-connection`**: Test source and destination connections
  ```bash
  dataloader test-connection recipe.yaml
  ```

- **`dataloader dry-run`**: Simulate execution without writing data
  ```bash
  dataloader dry-run recipe.yaml
  ```

- **`dataloader resume`**: Resume a failed execution
  ```bash
  dataloader resume recipe.yaml
  ```

- **`dataloader cancel`**: Mark a recipe as canceled
  ```bash
  dataloader cancel my_recipe
  ```

### CLI Options

- `--state-dir`: Directory for local state files (default: `.state`)
- `--state-backend`: State backend config (e.g., `s3://bucket/prefix`, `dynamodb:table`)
- `--vars`: CLI variables in `key=value` format (can be used multiple times)
- `--log-level`: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
- `--json-logs`: Use JSON format for logs

## Parallelism

Recipes can be configured for parallel batch processing using the `parallelism` field in `runtime`:

```yaml
runtime:
  batch_size: 10000
  parallelism: 4  # Process 4 batches concurrently
```

- **parallelism: 1** (default): Sequential processing, one batch at a time
- **parallelism > 1**: Async parallel processing using asyncio

Parallel execution uses async/await with semaphore-based concurrency control to ensure safe state updates.

## Logging & Metrics

### Structured Logging

DataLoader supports both JSON and normal log formats:

```python
from dataloader.core.logging import configure_logging

# Normal format (default)
configure_logging(level="INFO")

# JSON format
configure_logging(level="INFO", json_format=True)
```

Logs include context such as recipe name, batch ID, and execution details.

### Metrics Collection

Metrics are automatically collected during execution:

- Batches processed
- Rows processed
- Errors
- Execution time
- Rows per second
- Average batch time

Metrics are saved to state metadata and can be accessed via the state backend.

## State Management

State is persisted between runs to enable incremental loads. State includes:

- **cursor_values**: Last processed cursor values for incremental loads
- **watermarks**: High watermarks for time-based incremental loads
- **checkpoints**: Recovery checkpoints
- **metadata**: Additional state metadata (including metrics)

### State Backends

DataLoader supports multiple state backends:

- **LocalStateBackend** (default): Stores state in JSON files under `.state/`
  ```python
  from dataloader.core.state_backend import LocalStateBackend
  backend = LocalStateBackend(".state")
  ```

- **S3StateBackend**: Stores state in S3
  ```python
  from dataloader.core.state_backend import S3StateBackend
  backend = S3StateBackend(bucket="my-bucket", prefix="state/")
  ```

- **DynamoDBStateBackend**: Stores state in DynamoDB
  ```python
  from dataloader.core.state_backend import DynamoDBStateBackend
  backend = DynamoDBStateBackend(table_name="dataloader-state")
  ```

### State Backend Factory

Use the factory function to create backends from config strings:

```python
from dataloader.core.state_backend import create_state_backend

# Local
backend = create_state_backend("local:.state")

# S3
backend = create_state_backend("s3://my-bucket/state/")

# DynamoDB
backend = create_state_backend("dynamodb:my-table")
backend = create_state_backend("dynamodb:my-table:us-east-1")  # with region
```

## Development

### Running Tests

```bash
pytest
```

### Running Integration Tests

```bash
pytest tests/integration/
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please read our [Contributing Guidelines](CONTRIBUTING.md) for details on our code of conduct and the process for submitting pull requests.
