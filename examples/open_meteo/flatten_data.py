"""Custom transform to flatten Open Meteo API daily response into rows."""

from __future__ import annotations

from typing import Any

from dataloader.core.batch import ArrowBatch, Batch
from dataloader.core.exceptions import TransformError
from dataloader.transforms.registry import BaseTransform, register_transform


def flatten_open_meteo_daily(response: dict[str, Any]) -> list[dict[str, Any]]:
    """Flatten Open Meteo daily forecast response into rows.

    Args:
        response: Open Meteo API response containing "daily" object with arrays.

    Returns:
        List of dictionaries, one per day, with flattened structure.
    """
    if "daily" not in response:
        raise TransformError("Response must contain 'daily' key")

    daily = response["daily"]
    if not isinstance(daily, dict):
        raise TransformError("daily must be an object with arrays")

    if "time" not in daily:
        raise TransformError("Daily data must contain 'time' array")

    time_array = daily["time"]
    num_days = len(time_array)

    rows: list[dict[str, Any]] = []
    for i in range(num_days):
        row = {"date": time_array[i]}
        for key, value in daily.items():
            if key == "time":
                continue
            if isinstance(value, list):
                row[key] = value[i] if i < len(value) else None
        rows.append(row)

    return rows


class FlattenOpenMeteoDailyTransform(BaseTransform):
    """Transform that flattens Open Meteo daily data in batch rows."""

    def __init__(self, config: dict[str, Any]):
        self._source_field = config.get("source_field", "daily")

    def apply(self, batch: Batch) -> Batch:
        if not isinstance(batch, ArrowBatch):
            raise TransformError("Flatten transform requires ArrowBatch")

        table = batch.to_arrow()
        
        # When data_path=daily extracts a dict-of-arrays, the connector wraps it in a list.
        # PyArrow creates a table with one row where each column contains an array value.
        if len(table) == 0:
            return batch
            
        if len(table) > 1:
            raise TransformError("Flatten transform expects single row with dict-of-arrays structure")
        
        # Build dict-of-arrays from the first (and only) row
        # Each column's first value should be the array
        row_dict: dict[str, Any] = {}
        for col_name in table.column_names:
            col = table[col_name]
            # Get the first (and only) value from this column, which should be an array
            value = col[0]
            # Convert Arrow array/list to Python list
            if hasattr(value, 'as_py'):
                row_dict[col_name] = value.as_py()
            elif hasattr(value, 'tolist'):
                row_dict[col_name] = value.tolist()
            else:
                row_dict[col_name] = value
        
        # Support two shapes:
        # 1) row contains nested "daily" dict-of-arrays (source_field present)
        # 2) row is the dict-of-arrays itself (result of data_path=daily)
        daily = row_dict.get(self._source_field)
        if daily is None:
            # Check if row_dict itself is the dict-of-arrays (has "time" and arrays)
            if "time" in row_dict and isinstance(row_dict["time"], list):
                daily = row_dict
        
        if daily is None:
            # Passthrough if we can't find daily data
            return batch

        # Ensure daily is a dict with list values
        if not isinstance(daily, dict):
            raise TransformError(f"Expected dict-of-arrays, got {type(daily)}")
        
        daily_dict: dict[str, Any] = {}
        for key, value in daily.items():
            # Ensure values are Python lists
            if isinstance(value, list):
                daily_dict[key] = value
            elif hasattr(value, 'as_py'):
                daily_dict[key] = value.as_py()
            elif hasattr(value, 'tolist'):
                daily_dict[key] = value.tolist()
            else:
                daily_dict[key] = value

        flattened = flatten_open_meteo_daily({"daily": daily_dict})
        
        if not flattened:
            return batch
        
        # Extract columns in consistent order (date first, then others)
        columns = list(flattened[0].keys())
        
        # Convert flattened rows to ArrowBatch format (list of lists)
        rows = [[row[col] for col in columns] for row in flattened]
        
        return ArrowBatch.from_rows(
            columns=columns,
            rows=rows,
            metadata=getattr(batch, "metadata", {})
        )


@register_transform("flatten_open_meteo_daily")
def create_flatten_open_meteo_daily_transform(config: dict[str, Any]) -> FlattenOpenMeteoDailyTransform:
    """Factory for FlattenOpenMeteoDailyTransform."""
    return FlattenOpenMeteoDailyTransform(config)

