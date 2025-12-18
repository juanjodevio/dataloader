"""Cast columns transform implementation."""

from datetime import datetime
from typing import Any

from dataloader.core.batch import Batch, DictBatch
from dataloader.core.exceptions import TransformError
from dataloader.transforms.registry import register_transform

# Supported type converters
_TYPE_CONVERTERS: dict[str, type | Any] = {
    "str": str,
    "int": int,
    "float": float,
    "datetime": lambda v: _parse_datetime(v),
}


def _parse_datetime(value: Any) -> datetime:
    """Parse datetime from ISO format string or return if already datetime."""
    if isinstance(value, datetime):
        return value
    if value is None:
        return None
    try:
        return datetime.fromisoformat(str(value))
    except ValueError as e:
        raise ValueError(f"Cannot parse datetime from '{value}': {e}") from e


class CastColumnsTransform:
    """Casts column values to specified types.

    Config:
        columns: dict mapping column names to target types
                 e.g., {"age": "int", "price": "float", "created_at": "datetime"}

    Supported types: str, int, float, datetime (ISO format)
    """

    def __init__(self, config: dict[str, Any]):
        """Initialize with column type mapping.

        Args:
            config: Must contain 'columns' dict with column->type mappings.

        Raises:
            TransformError: If columns config is missing or contains invalid types.
        """
        columns = config.get("columns")
        if not columns:
            raise TransformError(
                "cast requires 'columns' configuration",
                context={"config": config},
            )
        if not isinstance(columns, dict):
            raise TransformError(
                "cast 'columns' must be a dictionary",
                context={"columns_type": type(columns).__name__},
            )

        self._validate_types(columns)
        self._columns = columns

    def _validate_types(self, columns: dict[str, str]) -> None:
        """Validate all specified types are supported."""
        invalid_types = [
            (col, typ)
            for col, typ in columns.items()
            if typ not in _TYPE_CONVERTERS
        ]
        if invalid_types:
            raise TransformError(
                f"Unsupported types: {invalid_types}",
                context={
                    "invalid_types": invalid_types,
                    "supported_types": list(_TYPE_CONVERTERS.keys()),
                },
            )

    def apply(self, batch: Batch) -> Batch:
        """Apply type casts to specified columns.

        Args:
            batch: Input batch with columns to cast.

        Returns:
            New batch with converted column values.

        Raises:
            TransformError: If column doesn't exist or conversion fails.
        """
        self._validate_columns_exist(batch.columns)

        column_indices = {col: i for i, col in enumerate(batch.columns)}
        cast_indices = {
            column_indices[col]: _TYPE_CONVERTERS[typ]
            for col, typ in self._columns.items()
        }

        new_rows = []
        for row_index, row in enumerate(batch.rows):
            new_row = self._cast_row(row, cast_indices, row_index)
            new_rows.append(new_row)

        return DictBatch(
            columns=batch.columns.copy(),
            rows=new_rows,
            metadata=batch.metadata.copy(),
        )

    def _cast_row(
        self,
        row: list[Any],
        cast_indices: dict[int, Any],
        row_index: int,
    ) -> list[Any]:
        """Cast values in a single row."""
        new_row = row.copy()
        for col_index, converter in cast_indices.items():
            original_value = row[col_index]
            try:
                if original_value is not None:
                    new_row[col_index] = converter(original_value)
            except (ValueError, TypeError) as e:
                raise TransformError(
                    f"Cast failed for row {row_index}",
                    context={
                        "row_index": row_index,
                        "column_index": col_index,
                        "original_value": original_value,
                        "error": str(e),
                    },
                ) from e
        return new_row

    def _validate_columns_exist(self, columns: list[str]) -> None:
        """Validate all target columns exist in batch."""
        column_set = set(columns)
        missing = [col for col in self._columns.keys() if col not in column_set]
        if missing:
            raise TransformError(
                f"Columns not found in batch: {missing}",
                context={
                    "missing_columns": missing,
                    "available_columns": columns,
                },
            )


@register_transform("cast")
def create_cast_transform(config: dict[str, Any]) -> CastColumnsTransform:
    """Factory function for CastColumnsTransform."""
    return CastColumnsTransform(config)

