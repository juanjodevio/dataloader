"""Schema evolution utilities (Phase 2, dlt-inspired)."""

from __future__ import annotations

from typing import Dict, List, Tuple

from pydantic import BaseModel, Field

from dataloader.core.schema.models import Column, Schema


class SchemaUpdate(BaseModel):
    added_columns: List[str] = Field(default_factory=list)
    removed_columns: List[str] = Field(default_factory=list)
    type_changes: List[str] = Field(default_factory=list)
    variant_columns: List[str] = Field(default_factory=list)


class SchemaEvolution:
    def __init__(self, variant_suffix_fmt: str = "__v_{type}") -> None:
        self.variant_suffix_fmt = variant_suffix_fmt

    def apply(self, current: Schema, incoming: Schema) -> Tuple[Schema, SchemaUpdate]:
        """Apply evolution rules: add new columns, create variants for type changes."""
        update = SchemaUpdate()
        current_map: Dict[str, Column] = {c.name: c for c in current.columns}
        new_columns: List[Column] = list(current.columns)

        for col in incoming.columns:
            existing = current_map.get(col.name)
            if existing is None:
                new_columns.append(col)
                update.added_columns.append(col.name)
                continue

            if existing.type != col.type:
                variant_name = self._variant_name(col.name, col.type)
                if variant_name not in current_map:
                    variant_col = Column(
                        name=variant_name,
                        type=col.type,
                        nullable=col.nullable,
                        metadata={**col.metadata, "variant_of": col.name},
                        lineage={"data_type": col.type, **col.lineage},
                        variant_of=col.name,
                    )
                    new_columns.append(variant_col)
                    current_map[variant_name] = variant_col
                    update.variant_columns.append(variant_name)
                update.type_changes.append(col.name)

        incoming_names = {c.name for c in incoming.columns}
        for col in current.columns:
            if col.name not in incoming_names:
                update.removed_columns.append(col.name)

        updated_schema = current.model_copy(update={"columns": new_columns})
        return updated_schema, update

    def _variant_name(self, base: str, type_str: str) -> str:
        safe_type = type_str.replace(" ", "_").replace("<", "_").replace(">", "_")
        return f"{base}{self.variant_suffix_fmt.format(type=safe_type)}"

