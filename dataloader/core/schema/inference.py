"""Schema inference utilities (Phase 2, dlt-inspired)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Literal

import pyarrow as pa

from dataloader.core.schema.models import Column, Schema
from dataloader.core.type_mapping import arrow_type_to_string

ReflectionLevel = Literal["none", "shallow", "one", "deep", "full"]


@dataclass
class InferenceResult:
    schema: Schema
    reflection_level: ReflectionLevel
    sample_size: int | None


class TypeInferrer:
    def __init__(
        self,
        reflection_level: ReflectionLevel = "deep",
        sample_size: int | None = None,
    ) -> None:
        self.reflection_level = reflection_level
        self.sample_size = sample_size

    def infer(self, table: pa.Table) -> InferenceResult:
        """Infer schema from an Arrow table respecting reflection depth."""
        if self.sample_size is not None and table.num_rows > self.sample_size:
            table = table.slice(0, self.sample_size)

        level = self.reflection_level
        if level == "none":
            cols = self._from_arrow_schema(table.schema)
        elif level in ("shallow", "one"):
            cols = self._from_arrow_schema(table.schema)
        else:
            cols = list(self._flatten_schema(table.schema))

        schema = Schema(columns=cols)
        return InferenceResult(
            schema=schema, reflection_level=level, sample_size=self.sample_size
        )

    def _from_arrow_schema(self, schema: pa.Schema) -> List[Column]:
        return [
            Column(
                name=field.name,
                type=arrow_type_to_string(field.type),
                nullable=field.nullable,
                metadata={"source_path": field.name, "inferred": True},
                lineage={"data_type": arrow_type_to_string(field.type)},
            )
            for field in schema
        ]

    def _flatten_schema(self, schema: pa.Schema) -> Iterable[Column]:
        for field in schema:
            yield from self._flatten_field(
                prefix=field.name, field_type=field.type, nullable=field.nullable
            )

    def _flatten_field(
        self, prefix: str, field_type: pa.DataType, nullable: bool
    ) -> Iterable[Column]:
        if pa.types.is_struct(field_type):
            for child in field_type:
                child_prefix = f"{prefix}__{child.name}"
                yield from self._flatten_field(child_prefix, child.type, child.nullable)
            return

        if pa.types.is_list(field_type) or pa.types.is_large_list(field_type):
            value_type = field_type.value_type
            list_prefix = f"{prefix}__item"
            if pa.types.is_struct(value_type):
                for child in value_type:
                    child_prefix = f"{list_prefix}__{child.name}"
                    yield from self._flatten_field(
                        child_prefix, child.type, child.nullable
                    )
                return
            col_type = f"list<{arrow_type_to_string(value_type)}>"
            yield Column(
                name=prefix,
                type=col_type,
                nullable=nullable,
                metadata={"source_path": prefix, "inferred": True},
                lineage={"data_type": col_type},
            )
            return

        col_type = arrow_type_to_string(field_type)
        yield Column(
            name=prefix,
            type=col_type,
            nullable=nullable,
            metadata={"source_path": prefix, "inferred": True},
            lineage={"data_type": col_type},
        )
