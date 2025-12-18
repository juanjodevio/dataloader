# DataLoader

Recipe-driven data loading framework for declarative EL (Extract-Load) workflows.

DataLoader enables you to define data pipelines using simple YAML recipes, handling reliability, batching, retries, and state management automatically. Define what you want to sync, not how.

## Features

- **Declarative Recipes**: Define data pipelines in YAML with inheritance support
- **Incremental Loads**: Automatic cursor-based incremental loading with state persistence
- **Transform Pipeline**: Built-in transforms (rename, cast, add columns) with extensible architecture
- **Multiple Connectors**: Support for Postgres, DuckDB, S3, CSV, and more
- **State Management**: Persistent state for resumable and incremental loads
- **Template System**: Environment variables and recipe metadata in configurations

## Installation

Install the package in development mode:

```bash
pip install -e .
```

Or install dependencies:

```bash
pip install -r requirements.txt
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
  path: "{{ env.CSV_PATH | default('data/input.csv') }}"

transform:
  steps: []

destination:
  type: s3
  bucket: "{{ env.S3_BUCKET | default('my-bucket') }}"
  path: "{{ env.S3_PATH | default('output/data.csv') }}"
  region: "{{ env.AWS_REGION | default('us-east-1') }}"
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
- **Postgres**: PostgreSQL databases with SQLAlchemy
- **CSV**: Local CSV files
- **S3**: S3 objects via boto3 and fsspec

### Destinations
- **DuckDB**: DuckDB databases (file-based or in-memory)
- **S3**: S3 destinations with various formats

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

## State Management

State is persisted between runs to enable incremental loads. State includes:

- **cursor_values**: Last processed cursor values for incremental loads
- **watermarks**: High watermarks for time-based incremental loads
- **checkpoints**: Recovery checkpoints
- **metadata**: Additional state metadata

State is stored using a `StateBackend`. The default `LocalStateBackend` stores state in JSON files under `.state/`.

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

[Add license information]

## Contributing

[Add contributing guidelines]
