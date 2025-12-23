"""Column lineage tracking."""

from __future__ import annotations

from typing import Dict, List, Optional

from pydantic import BaseModel, Field

from dataloader.core.schema.models import Schema


class ColumnLineageEntry(BaseModel):
    version: str
    data_type: str
    variant_of: Optional[str] = None


class SchemaLineage:
    def __init__(self) -> None:
        # recipe -> column -> entries
        self._data: Dict[str, Dict[str, List[ColumnLineageEntry]]] = {}

    def record(self, recipe: str, version: str, schema: Schema) -> None:
        per_recipe = self._data.setdefault(recipe, {})
        for col in schema.columns:
            entries = per_recipe.setdefault(col.name, [])
            # avoid duplicate entries for same version/type
            if any(e.version == version and e.data_type == col.type for e in entries):
                continue
            entries.append(
                ColumnLineageEntry(
                    version=version,
                    data_type=col.type,
                    variant_of=col.variant_of,
                )
            )

    def get(self, recipe: str, column: str) -> List[ColumnLineageEntry]:
        return list(self._data.get(recipe, {}).get(column, []))
