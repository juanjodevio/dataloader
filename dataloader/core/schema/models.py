"""Schema models for data validation and type management.

This module provides schema definitions, validation modes, and evolution
policies for managing data schemas across the data loading pipeline.

NOTE: This is a temporary implementation for Phase 1. Schema evolution
will be redesigned in Phase 2 to align with dlt's approach, including:
- Variant columns for type changes (e.g., column__v_text)
- Schema contracts with modes (evolve, freeze, discard_rows, discard_columns)
- Column lineage tracking
- Better handling of removed/renamed columns
"""

from enum import Enum
from typing import Any, Optional

import pyarrow as pa
from pydantic import BaseModel, Field


class SchemaMode(str, Enum):
    """Schema validation and inference mode."""

    STRICT = "strict"  # Fail on schema mismatches
    LENIENT = "lenient"  # Warn and coerce when possible
    INFER = "infer"  # Automatically infer and update schema


class Column(BaseModel):
    """Schema definition for a single column."""

    name: str = Field(description="Column name")
    type: str = Field(description="Column type (e.g., 'int', 'str', 'datetime')")
    nullable: bool = Field(
        default=True, description="Whether column allows null values"
    )
    primary_key: bool = Field(
        default=False, description="Whether column is a primary key"
    )
    unique: bool = Field(
        default=False, description="Whether column values must be unique"
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict, description="Additional column metadata"
    )
    variant_of: Optional[str] = Field(
        default=None,
        description="Base column name if this is a variant column created due to type change",
    )
    lineage: dict[str, Any] = Field(
        default_factory=dict,
        description="Lineage metadata (e.g., add_time, data_type, load_id)",
    )


class EvolutionPolicy(BaseModel):
    """Policy for handling schema evolution."""

    allow_new_columns: bool = Field(
        default=True, description="Allow new columns to be added to schema"
    )
    allow_column_deletion: bool = Field(
        default=False, description="Allow columns to be removed from schema"
    )
    allow_type_changes: bool = Field(
        default=False, description="Allow column types to change"
    )


class Schema(BaseModel):
    """Complete schema definition for a dataset."""

    columns: list[Column] = Field(description="List of column definitions")
    mode: SchemaMode = Field(
        default=SchemaMode.INFER, description="Schema validation mode"
    )
    evolution: EvolutionPolicy = Field(
        default_factory=EvolutionPolicy, description="Schema evolution policy"
    )
    version: Optional[str] = Field(
        default=None, description="Schema version identifier"
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict, description="Additional schema metadata"
    )

    def get_column(self, name: str) -> Optional[Column]:
        for col in self.columns:
            if col.name == name:
                return col
        return None

    def get_column_names(self) -> list[str]:
        return [col.name for col in self.columns]

    def to_arrow_schema(self) -> pa.Schema:
        from dataloader.core.type_mapping import string_to_arrow_type

        fields = []
        for col in self.columns:
            arrow_type = string_to_arrow_type(col.type)
            if not col.nullable:
                pass
            fields.append(pa.field(col.name, arrow_type, nullable=col.nullable))

        return pa.schema(fields)

    @classmethod
    def from_arrow_schema(cls, arrow_schema: pa.Schema) -> "Schema":
        from dataloader.core.type_mapping import arrow_type_to_string

        columns = []
        for field in arrow_schema:
            col_type = arrow_type_to_string(field.type)
            columns.append(
                Column(
                    name=field.name,
                    type=col_type,
                    nullable=field.nullable,
                )
            )

        return cls(columns=columns)
