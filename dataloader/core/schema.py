"""Schema models for data validation and type management.

This module provides schema definitions, validation modes, and evolution
policies for managing data schemas across the data loading pipeline.
"""

from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field

import pyarrow as pa


class SchemaMode(str, Enum):
    """Schema validation and inference mode."""

    STRICT = "strict"  # Fail on schema mismatches
    LENIENT = "lenient"  # Warn and coerce when possible
    INFER = "infer"  # Automatically infer and update schema


class Column(BaseModel):
    """Schema definition for a single column.

    Defines the expected type, nullability, and constraints for a column.
    """

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


class EvolutionPolicy(BaseModel):
    """Policy for handling schema evolution.

    Defines rules for how schemas can change over time.
    """

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
    """Complete schema definition for a dataset.

    Contains column definitions, metadata, and version information.
    """

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
        """Get column schema by name.

        Args:
            name: Column name

        Returns:
            Column if found, None otherwise
        """
        for col in self.columns:
            if col.name == name:
                return col
        return None

    def get_column_names(self) -> list[str]:
        """Get list of column names in schema.

        Returns:
            List of column names
        """
        return [col.name for col in self.columns]

    def to_arrow_schema(self) -> pa.Schema:
        """Convert schema to PyArrow Schema.

        Returns:
            PyArrow Schema object
        """
        from dataloader.core.type_mapping import string_to_arrow_type

        fields = []
        for col in self.columns:
            arrow_type = string_to_arrow_type(col.type)
            # Arrow types are nullable by default, but we can make them non-nullable
            # if the schema specifies it
            if not col.nullable:
                # Note: Arrow doesn't support non-nullable types directly in all cases
                # This is a simplified approach - full implementation would need
                # to handle this more carefully
                pass
            fields.append(pa.field(col.name, arrow_type, nullable=col.nullable))

        return pa.schema(fields)

    @classmethod
    def from_arrow_schema(cls, arrow_schema: pa.Schema) -> "Schema":
        """Create Schema from PyArrow Schema.

        Args:
            arrow_schema: PyArrow Schema object

        Returns:
            Schema instance
        """
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
