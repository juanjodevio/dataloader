"""Schema storage interfaces."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Optional, Protocol

from dataloader.core.schema.models import Schema


class SchemaStorage(Protocol):
    def save(self, recipe: str, version: str, schema: Schema) -> None: ...

    def load(self, recipe: str, version: str) -> Optional[Schema]: ...

    def list_versions(self, recipe: str) -> List[str]: ...


class InMemorySchemaStorage:
    def __init__(self) -> None:
        self._store: Dict[str, Dict[str, Schema]] = {}

    def save(self, recipe: str, version: str, schema: Schema) -> None:
        self._store.setdefault(recipe, {})[version] = schema

    def load(self, recipe: str, version: str) -> Optional[Schema]:
        return self._store.get(recipe, {}).get(version)

    def list_versions(self, recipe: str) -> List[str]:
        return list(self._store.get(recipe, {}).keys())


class LocalJsonSchemaStorage:
    """Simple JSON file storage for schemas."""

    def __init__(self, base_path: Path) -> None:
        self.base_path = base_path
        self.base_path.mkdir(parents=True, exist_ok=True)

    def _path(self, recipe: str, version: str) -> Path:
        return self.base_path / recipe / f"{version}.json"

    def save(self, recipe: str, version: str, schema: Schema) -> None:
        path = self._path(recipe, version)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(schema.model_dump_json(), encoding="utf-8")

    def load(self, recipe: str, version: str) -> Optional[Schema]:
        path = self._path(recipe, version)
        if not path.exists():
            return None
        data = json.loads(path.read_text(encoding="utf-8"))
        return Schema.model_validate(data)

    def list_versions(self, recipe: str) -> List[str]:
        recipe_dir = self.base_path / recipe
        if not recipe_dir.exists():
            return []
        return [p.stem for p in recipe_dir.glob("*.json")]
