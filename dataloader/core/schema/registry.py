"""Schema registry: versioned storage with lineage tracking."""

from __future__ import annotations

import datetime as dt
from typing import List, Optional, Tuple

from dataloader.core.schema.evolution import SchemaEvolution, SchemaUpdate
from dataloader.core.schema.lineage import SchemaLineage
from dataloader.core.schema.models import Schema
from dataloader.core.schema.storage import InMemorySchemaStorage, SchemaStorage


class SchemaRegistry:
    """Registry for versioned schema storage with lineage tracking.

    Manages schema versions, evolution, and lineage for recipes.
    """

    def __init__(
        self,
        storage: Optional[SchemaStorage] = None,
        lineage: Optional[SchemaLineage] = None,
        evolution: Optional[SchemaEvolution] = None,
    ) -> None:
        """Initialize schema registry.

        Args:
            storage: Schema storage backend (defaults to InMemorySchemaStorage).
            lineage: Schema lineage tracker (defaults to SchemaLineage).
            evolution: Schema evolution handler (defaults to SchemaEvolution).
        """
        self.storage = storage or InMemorySchemaStorage()
        self.lineage = lineage or SchemaLineage()
        self.evolution = evolution or SchemaEvolution()

    def register(
        self, recipe: str, schema: Schema, version: Optional[str] = None
    ) -> str:
        """Register a schema version for a recipe.

        Args:
            recipe: Recipe name.
            schema: Schema to register.
            version: Optional version string (auto-generated if not provided).

        Returns:
            Version ID for the registered schema.
        """
        version_id = version or self._timestamp_version()

        # If existing version present, optionally evolve?
        self.storage.save(recipe, version_id, schema)
        self.lineage.record(recipe, version_id, schema)
        return version_id

    def get(self, recipe: str, version: str) -> Optional[Schema]:
        """Get a schema version for a recipe.

        Args:
            recipe: Recipe name.
            version: Version string.

        Returns:
            Schema if found, None otherwise.
        """
        return self.storage.load(recipe, version)

    def list_versions(self, recipe: str) -> List[str]:
        """List all versions for a recipe.

        Args:
            recipe: Recipe name.

        Returns:
            List of version strings.
        """
        return self.storage.list_versions(recipe)

    def compare(
        self, recipe: str, base_version: str, new_schema: Schema
    ) -> Optional[SchemaUpdate]:
        """Compare new schema against a base version.

        Args:
            recipe: Recipe name.
            base_version: Base version to compare against.
            new_schema: New schema to compare.

        Returns:
            SchemaUpdate if base version exists, None otherwise.
        """
        current = self.get(recipe, base_version)
        if current is None:
            return None
        _, update = self.evolution.apply(current, new_schema)
        return update

    def _timestamp_version(self) -> str:
        """Generate timestamp-based version string.

        Returns:
            Version string in format YYYYMMDDTHHMMSSffffffZ.
        """
        return dt.datetime.utcnow().strftime("%Y%m%dT%H%M%S%fZ")
