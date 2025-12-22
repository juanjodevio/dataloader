"""Schema registry: versioned storage with lineage tracking."""

from __future__ import annotations

import datetime as dt
from typing import List, Optional, Tuple

from dataloader.core.schema.evolution import SchemaEvolution, SchemaUpdate
from dataloader.core.schema.lineage import SchemaLineage
from dataloader.core.schema.models import Schema
from dataloader.core.schema.storage import InMemorySchemaStorage, SchemaStorage


class SchemaRegistry:
    def __init__(
        self,
        storage: Optional[SchemaStorage] = None,
        lineage: Optional[SchemaLineage] = None,
        evolution: Optional[SchemaEvolution] = None,
    ) -> None:
        self.storage = storage or InMemorySchemaStorage()
        self.lineage = lineage or SchemaLineage()
        self.evolution = evolution or SchemaEvolution()

    def register(self, recipe: str, schema: Schema, version: Optional[str] = None) -> str:
        version_id = version or self._timestamp_version()

        # If existing version present, optionally evolve?
        self.storage.save(recipe, version_id, schema)
        self.lineage.record(recipe, version_id, schema)
        return version_id

    def get(self, recipe: str, version: str) -> Optional[Schema]:
        return self.storage.load(recipe, version)

    def list_versions(self, recipe: str) -> List[str]:
        return self.storage.list_versions(recipe)

    def compare(self, recipe: str, base_version: str, new_schema: Schema) -> Optional[SchemaUpdate]:
        current = self.get(recipe, base_version)
        if current is None:
            return None
        _, update = self.evolution.apply(current, new_schema)
        return update

    def _timestamp_version(self) -> str:
        return dt.datetime.utcnow().strftime("%Y%m%dT%H%M%S%fZ")

