"""Add column transform implementation."""

import re
from datetime import datetime
from typing import Any

import pyarrow as pa

from dataloader.core.batch import ArrowBatch, Batch
from dataloader.core.exceptions import TransformError
from dataloader.transforms.registry import register_transform


class AddColumnTransform:
    """Adds a new column to a batch with a constant or template value.

    Config:
        name: str - Name of the new column
        value: Any - Constant value or template string (e.g., "{{ recipe.name }}")

    Template support:
        - "{{ recipe.name }}" renders to recipe name from context
    """

    def __init__(self, config: dict[str, Any]):
        """Initialize with column name and value.

        Args:
            config: Must contain 'name' (str) and 'value' (Any).

        Raises:
            TransformError: If required config is missing.
        """
        name = config.get("name")
        if not name:
            raise TransformError(
                "add_column requires 'name' configuration",
                context={"config": config},
            )
        if not isinstance(name, str):
            raise TransformError(
                "add_column 'name' must be a string",
                context={"name_type": type(name).__name__},
            )

        if "value" not in config:
            raise TransformError(
                "add_column requires 'value' configuration",
                context={"config": config},
            )

        self._name = name
        self._value = config["value"]
        self._context = config.get("context", {})

    def apply(self, batch: Batch) -> Batch:
        """Add new column to batch with constant value using Arrow-native operations.

        Args:
            batch: Input batch to extend.

        Returns:
            New batch with added column.

        Raises:
            TransformError: If column already exists.
        """
        if not isinstance(batch, ArrowBatch):
            raise TransformError(
                "AddColumnTransform requires ArrowBatch",
                context={"batch_type": type(batch).__name__},
            )

        if self._name in batch.columns:
            raise TransformError(
                f"Column '{self._name}' already exists",
                context={
                    "column_name": self._name,
                    "existing_columns": batch.columns,
                },
            )

        resolved_value = self._resolve_value(self._value)

        # Get Arrow table
        arrow_table = batch.to_arrow()

        # Create a new column array with constant value for all rows
        num_rows = len(arrow_table)
        
        # Infer Arrow type from the value
        arrow_type = self._infer_arrow_type(resolved_value)
        
        # Create array with constant value
        if resolved_value is None:
            new_array = pa.nulls(num_rows, type=arrow_type)
        else:
            # Create array by repeating the value
            new_array = pa.array([resolved_value] * num_rows, type=arrow_type)

        # Append column to table
        new_field = pa.field(self._name, arrow_type, nullable=True)
        new_table = arrow_table.append_column(new_field, new_array)

        return ArrowBatch(
            new_table,
            metadata=batch.metadata.copy(),
        )

    def _infer_arrow_type(self, value: Any) -> pa.DataType:
        """Infer Arrow type from Python value."""
        if value is None:
            return pa.null()
        elif isinstance(value, bool):
            return pa.bool_()
        elif isinstance(value, int):
            return pa.int64()
        elif isinstance(value, float):
            return pa.float64()
        elif isinstance(value, str):
            return pa.string()
        elif isinstance(value, datetime):
            return pa.timestamp("us")
        else:
            # Default to string for unknown types
            return pa.string()

    def _resolve_value(self, value: Any) -> Any:
        """Resolve value, handling template expressions.

        For v0.1, supports only:
        - Constant values (returned as-is)
        - {{ recipe.name }} template (if context provided)
        """
        if not isinstance(value, str):
            return value

        pattern = r"\{\{\s*([^}]+)\s*\}\}"
        match = re.search(pattern, value)
        if not match:
            return value

        expr = match.group(1).strip()
        return self._evaluate_template(expr, value)

    def _evaluate_template(self, expr: str, original: str) -> Any:
        """Evaluate a single template expression."""
        parts = expr.split(".")
        if len(parts) == 2 and parts[0] == "recipe" and parts[1] == "name":
            recipe_context = self._context.get("recipe", {})
            if "name" in recipe_context:
                return recipe_context["name"]
            # Return original template if context not available
            return original

        # Unknown expression, return original value
        return original


@register_transform("add_column")
def create_add_column_transform(config: dict[str, Any]) -> AddColumnTransform:
    """Factory function for AddColumnTransform."""
    return AddColumnTransform(config)

