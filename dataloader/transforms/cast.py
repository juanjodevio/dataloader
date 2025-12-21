"""Cast columns transform implementation."""

from datetime import datetime
from typing import Any

import pyarrow as pa

from dataloader.core.batch import ArrowBatch, Batch
from dataloader.core.exceptions import TransformError
from dataloader.core.type_mapping import string_to_arrow_type
from dataloader.transforms.registry import register_transform

# Supported type converters (for Python values)
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
        """Apply type casts to specified columns using Arrow-native operations.

        Args:
            batch: Input batch with columns to cast.

        Returns:
            New batch with converted column values.

        Raises:
            TransformError: If column doesn't exist or conversion fails.
        """
        if not isinstance(batch, ArrowBatch):
            raise TransformError(
                "CastColumnsTransform requires ArrowBatch",
                context={"batch_type": type(batch).__name__},
            )

        self._validate_columns_exist(batch.columns)

        # Get Arrow table
        arrow_table = batch.to_arrow()
        original_schema = arrow_table.schema

        # Build new schema with cast types
        new_fields = []
        for i, field in enumerate(original_schema):
            col_name = field.name
            if col_name in self._columns:
                target_type_str = self._columns[col_name]
                try:
                    target_arrow_type = string_to_arrow_type(target_type_str)
                except ValueError as e:
                    raise TransformError(
                        f"Unsupported type for casting: {target_type_str}",
                        context={"column": col_name, "target_type": target_type_str, "error": str(e)},
                    ) from e
                new_fields.append(pa.field(col_name, target_arrow_type, nullable=field.nullable))
            else:
                new_fields.append(field)

        new_schema = pa.schema(new_fields)

        # Use Arrow's cast method
        try:
            cast_table = arrow_table.cast(new_schema, safe=False)
        except (pa.ArrowInvalid, pa.ArrowTypeError) as e:
            raise TransformError(
                f"Cast failed: {e}",
                context={
                    "columns": self._columns,
                    "error": str(e),
                },
            ) from e

        return ArrowBatch(
            cast_table,
            metadata=batch.metadata.copy(),
        )


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

