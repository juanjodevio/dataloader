"""Rename columns transform implementation."""

from typing import Any

from dataloader.core.batch import Batch, DictBatch
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
        """Apply column renames to batch.

        Args:
            batch: Input batch with columns to rename.

        Returns:
            New batch with renamed columns.

        Raises:
            TransformError: If any mapping key doesn't exist in batch columns.
        """
        self._validate_columns_exist(batch.columns)

        new_columns = [
            self._mapping.get(col, col) for col in batch.columns
        ]

        return DictBatch(
            columns=new_columns,
            rows=[row.copy() for row in batch.rows],
            metadata=batch.metadata.copy(),
        )

    def _validate_columns_exist(self, columns: list[str]) -> None:
        """Validate all mapping keys exist in batch columns."""
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
    """Factory function for RenameColumnsTransform."""
    return RenameColumnsTransform(config)

