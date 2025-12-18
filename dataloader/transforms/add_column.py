"""Add column transform implementation."""

import re
from typing import Any

from dataloader.core.batch import Batch, DictBatch
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
        """Add new column to batch with constant value.

        Args:
            batch: Input batch to extend.

        Returns:
            New batch with added column.

        Raises:
            TransformError: If column already exists.
        """
        if self._name in batch.columns:
            raise TransformError(
                f"Column '{self._name}' already exists",
                context={
                    "column_name": self._name,
                    "existing_columns": batch.columns,
                },
            )

        resolved_value = self._resolve_value(self._value)

        new_columns = batch.columns.copy() + [self._name]
        new_rows = [row.copy() + [resolved_value] for row in batch.rows]

        return DictBatch(
            columns=new_columns,
            rows=new_rows,
            metadata=batch.metadata.copy(),
        )

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

