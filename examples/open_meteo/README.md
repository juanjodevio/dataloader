# Open Meteo API Example

This example demonstrates how to load weather forecast data from the Open Meteo API into various destinations using the DataLoader API connector, custom transforms, and recipe inheritance.

## Overview

The example fetches 7-day weather forecasts from Open Meteo for Berlin (coordinates: 52.52, 13.405) and loads the flattened data into:
- **DuckDB** (SQL database file)
- **Local FileStore** (JSONL format)

## Prerequisites

1. **Python dependencies** installed:
   ```bash
   pip install dataloader[api]
   ```
   Or if using the project:
   ```bash
   pip install -e .
   ```

## Example Structure

```
examples/open_meteo/
├── flatten_data.py           # Custom transform implementation
├── main.py                   # Entry point for running recipes
├── recipes/
│   ├── base_open_meteo.yml   # Base recipe with shared configuration
│   ├── api_to_duckdb.yml     # API → DuckDB pipeline
│   └── api_to_local.yml      # API → Local FileStore pipeline
└── README.md                 # This file
```

## Running the Example

From the project root:

```bash
# Run API to DuckDB (default)
python -m examples.open_meteo.main --recipe api_to_duckdb

# Run API to Local FileStore
python -m examples.open_meteo.main --recipe api_to_local
```

Or using the CLI directly:
```bash
# DuckDB
dataloader run examples/open_meteo/recipes/api_to_duckdb.yml

# FileStore
dataloader run examples/open_meteo/recipes/api_to_local.yml
```

## Features Demonstrated

This example showcases several key DataLoader features:

1. **API Connector**: Fetching data from REST APIs with JSONPath extraction (`data_path`)
2. **Custom Transforms**: Implementing and registering a custom transform class
3. **Custom Transform Loading**: Loading custom transforms from recipe via `runtime.custom_transforms`
4. **Recipe Inheritance**: Using base recipes to share common configuration
5. **Transform Pipeline**: Applying multiple transform steps in sequence
6. **Multiple Destinations**: Same source/transform with different destinations

## Open Meteo API Response Structure

The Open Meteo API returns data in a nested structure with arrays:
```json
{
  "daily": {
    "time": ["2024-01-01", "2024-01-02", ...],
    "temperature_2m_max": [10.5, 12.3, ...],
    "temperature_2m_min": [5.2, 7.1, ...],
    "precipitation_sum": [0.0, 5.2, ...],
    "weather_code": [61, 45, ...]
  }
}
```

## Data Transformation

The example uses a **custom transform** (`flatten_open_meteo_daily`) to convert the nested arrays into row format:

**Input** (dict-of-arrays):
```json
{
  "daily": {
    "time": ["2024-01-01", "2024-01-02"],
    "temperature_2m_max": [10.5, 12.3]
  }
}
```

**Output** (rows):
```json
[
  {"date": "2024-01-01", "temperature_2m_max": 10.5, ...},
  {"date": "2024-01-02", "temperature_2m_max": 12.3, ...}
]
```

### Custom Transform Implementation

The custom transform is implemented in `flatten_data.py`:

1. **Transform Class**: `FlattenOpenMeteoDailyTransform` inherits from `BaseTransform`
2. **Registration**: Registered via `@register_transform("flatten_open_meteo_daily")`
3. **Loading**: Loaded automatically from the recipe via `runtime.custom_transforms`

### Recipe Configuration

The base recipe (`base_open_meteo.yml`) includes:
- API source configuration with `data_path: daily` to extract the daily object
- Transform pipeline with `add_column` and `flatten_open_meteo_daily`
- Custom transform loading in `runtime.custom_transforms`

```yaml
runtime:
  custom_transforms:
    - ../flatten_data.py

transform:
  steps:
    - type: add_column
      name: _loaded_at
      value: "NOW"
    - type: flatten_open_meteo_daily
      source_field: daily
```

## Customizing the Example

### Change Location

Edit `examples/open_meteo/recipes/base_open_meteo.yml`:
```yaml
source:
  params:
    latitude: 40.7128  # New York
    longitude: -74.0060
```

### Change Forecast Parameters

Modify parameters in `base_open_meteo.yml`:
```yaml
source:
  params:
    daily: temperature_2m_max,temperature_2m_min,precipitation_sum,weather_code,wind_speed_10m_max
    forecast_days: 14  # 14-day forecast instead of 7
```

### Add New Destination

Create a new recipe file (e.g., `api_to_postgres.yml`) that extends the base:

```yaml
name: open_meteo_to_postgres
extends: base_open_meteo.yml

destination:
  type: postgres
  host: localhost
  port: 5432
  database: weather_db
  user: postgres
  password: postgres
  table: public.weather_forecast
  write_mode: overwrite
```

### Modify the Transform Pipeline

Edit `base_open_meteo.yml` to add or remove transform steps:

```yaml
transform:
  steps:
    - type: add_column
      name: _loaded_at
      value: "NOW"
    - type: flatten_open_meteo_daily
      source_field: daily
    - type: rename_columns  # Example: add another transform
      mapping:
        date: forecast_date
```

## Creating Custom Transforms

This example demonstrates how to create and use custom transforms. Here's the pattern:

### 1. Define the Transform Class

```python
from dataloader.core.batch import ArrowBatch, Batch
from dataloader.core.exceptions import TransformError
from dataloader.transforms.registry import BaseTransform, register_transform

class MyCustomTransform(BaseTransform):
    """Transform that implements your custom logic."""
    
    def __init__(self, config: dict[str, Any]):
        self._config = config
    
    def apply(self, batch: Batch) -> Batch:
        if not isinstance(batch, ArrowBatch):
            raise TransformError("Transform requires ArrowBatch")
        
        # Your transformation logic here
        # ...
        
        return transformed_batch

@register_transform("my_custom_transform")
def create_my_transform(config: dict[str, Any]) -> MyCustomTransform:
    """Factory function for the transform."""
    return MyCustomTransform(config)
```

### 2. Load in Recipe

Add to your recipe's `runtime` section:

```yaml
runtime:
  custom_transforms:
    - ../path/to/your_transform.py  # Relative to recipe file
```

### 3. Use in Transform Pipeline

Reference the transform by its registered name:

```yaml
transform:
  steps:
    - type: my_custom_transform
      # Your transform config here
```

See `flatten_data.py` for a complete working example.

## Output

### DuckDB Output

Running `api_to_duckdb` creates `./output/open_meteo.duckdb` with a `weather_forecast` table.

Query the data:
```python
import duckdb

conn = duckdb.connect("./output/open_meteo.duckdb")
result = conn.execute("SELECT * FROM weather_forecast").fetchall()
conn.close()
```

### FileStore Output

Running `api_to_local` creates `./output/open_meteo_forecast.jsonl` with one JSON object per line (JSONL format).

## Troubleshooting

1. **Module import errors**: Ensure you've installed the package (`pip install -e .`) or the package is in your PYTHONPATH
2. **Custom transform not found**: Verify the path in `runtime.custom_transforms` is correct relative to the recipe file
3. **API errors**: Check internet connection and Open Meteo API status
4. **Data format issues**: Ensure the custom transform handles the API response structure correctly

## References

- [Open Meteo API Documentation](https://open-meteo.com/en/docs)
- [DataLoader Documentation](../../README.md)

