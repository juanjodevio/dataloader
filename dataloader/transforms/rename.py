"""Rename columns transform implementation."""

from typing import Any

import pyarrow as pa

from dataloader.core.batch import ArrowBatch, Batch
from dataloader.core.exceptions import TransformError
from dataloader.transforms.registry import register_transform


class RenameColumnsTransform:
    """Renames columns in a batch according to a mapping.

    Config:
        mapping: dict mapping old column names to new column names
                 e.g., {"old_name": "new_name", "id": "user_id"}
    """

    def __init__(self, config: dict[str, Any]):
        """Initialize with rename mapping.

        Args:
            config: Must contain 'mapping' dict with old->new column names.

        Raises:
            TransformError: If mapping is missing or invalid.
        """
        mapping = config.get("mapping")
        if not mapping:
            raise TransformError(
                "rename_columns requires 'mapping' configuration",
                context={"config": config},
            )
        if not isinstance(mapping, dict):
            raise TransformError(
                "rename_columns 'mapping' must be a dictionary",
                context={"mapping_type": type(mapping).__name__},
            )
        self._mapping = mapping

    def apply(self, batch: Batch) -> Batch:
        """Apply column renames to batch using Arrow-native operations.

        Args:
            batch: Input batch with columns to rename.

        Returns:
            New batch with renamed columns.

        Raises:
            TransformError: If any mapping key doesn't exist in batch columns.
        """
        if not isinstance(batch, ArrowBatch):
            raise TransformError(
                "RenameColumnsTransform requires ArrowBatch",
                context={"batch_type": type(batch).__name__},
            )

        self._validate_columns_exist(batch.columns)

        # Get Arrow table
        arrow_table = batch.to_arrow()

        # Build new column names (apply mapping)
        new_columns = [self._mapping.get(col, col) for col in batch.columns]

        # Use Arrow's native rename_columns for zero-copy operation
        renamed_table = arrow_table.rename_columns(new_columns)

        return ArrowBatch(
            renamed_table,
            metadata=batch.metadata.copy(),
        )

    def _validate_columns_exist(self, columns: list[str]) -> None:
        """Validate all mapping keys exist in batch columns.

        Args:
            columns: List of column names in the batch.

        Raises:
            TransformError: If any mapping key doesn't exist in columns.
        """
        column_set = set(columns)
        missing = [key for key in self._mapping.keys() if key not in column_set]
        if missing:
            raise TransformError(
                f"Columns not found in batch: {missing}",
                context={
                    "missing_columns": missing,
                    "available_columns": columns,
                },
            )


@register_transform("rename_columns")
def create_rename_transform(config: dict[str, Any]) -> RenameColumnsTransform:
    """Factory function for RenameColumnsTransform.

    Args:
        config: Transform configuration containing 'mapping' dict.

    Returns:
        RenameColumnsTransform instance.
    """
    return RenameColumnsTransform(config)
