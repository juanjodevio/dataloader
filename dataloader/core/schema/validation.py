"""Schema validation with contract enforcement (Phase 3)."""

from __future__ import annotations

from typing import List, Optional

import pyarrow as pa
from pydantic import BaseModel, Field

from dataloader.core.schema.contracts import ContractMode, SchemaContracts
from dataloader.core.schema.models import Column, Schema, SchemaMode
from dataloader.core.type_mapping import arrow_type_to_string, string_to_arrow_type


class ValidationIssue(BaseModel):
    level: str
    message: str
    column: Optional[str] = None


class ValidationResult(BaseModel):
    errors: List[ValidationIssue] = Field(default_factory=list)
    warnings: List[ValidationIssue] = Field(default_factory=list)
    dropped_columns: List[str] = Field(default_factory=list)
    validated_schema: Schema

    @property
    def ok(self) -> bool:
        return not self.errors

    @property
    def schema(self) -> Schema:
        return self.validated_schema


class SchemaValidator:
    def __init__(
        self,
        mode: SchemaMode = SchemaMode.INFER,
        contracts: Optional[SchemaContracts] = None,
    ) -> None:
        self.mode = mode
        self.contracts = contracts or SchemaContracts()

    def validate(self, table: pa.Table, schema: Schema) -> ValidationResult:
        table_fields = {field.name: field for field in table.schema}
        schema_map = {col.name: col for col in schema.columns}

        errors: List[ValidationIssue] = []
        warnings: List[ValidationIssue] = []
        dropped: List[str] = []
        new_columns: List[Column] = list(schema.columns)

        # Extra columns in data
        for name, field in table_fields.items():
            if name in schema_map:
                continue
            mode = self.contracts.column_mode(name, arrow_type_to_string(field.type))
            if mode == ContractMode.DISCARD_COLUMNS:
                dropped.append(name)
                warnings.append(ValidationIssue(level="warning", message=f"dropped column '{name}'", column=name))
                continue
            if self.mode == SchemaMode.STRICT or mode == ContractMode.FREEZE:
                errors.append(ValidationIssue(level="error", message=f"unexpected column '{name}'", column=name))
                continue
            if self.mode == SchemaMode.INFER:
                new_columns.append(
                    Column(
                        name=name,
                        type=arrow_type_to_string(field.type),
                        nullable=field.nullable,
                        metadata={"source_path": name, "inferred": True},
                        lineage={"data_type": arrow_type_to_string(field.type)},
                    )
                )
                warnings.append(ValidationIssue(level="warning", message=f"added column '{name}' to schema", column=name))
            else:
                warnings.append(ValidationIssue(level="warning", message=f"extra column '{name}'", column=name))

        # Missing columns expected by schema
        for name, col in schema_map.items():
            if name in table_fields:
                continue
            mode = self.contracts.column_mode(name, col.type)
            if self.mode == SchemaMode.STRICT or mode == ContractMode.FREEZE:
                errors.append(ValidationIssue(level="error", message=f"missing column '{name}'", column=name))
            else:
                warnings.append(ValidationIssue(level="warning", message=f"missing column '{name}'", column=name))

        # Type mismatches on shared columns
        for name, field in table_fields.items():
            if name not in schema_map:
                continue
            expected = self._normalize_type(schema_map[name].type)
            actual = self._normalize_type(arrow_type_to_string(field.type))
            if expected == actual:
                continue
            mode = self.contracts.column_mode(name, actual)
            if mode == ContractMode.FREEZE:
                errors.append(
                    ValidationIssue(
                        level="error",
                        message=f"type mismatch for '{name}': expected {expected}, got {actual}",
                        column=name,
                    )
                )
                continue
            if self.mode == SchemaMode.STRICT:
                errors.append(
                    ValidationIssue(
                        level="error",
                        message=f"type mismatch for '{name}': expected {expected}, got {actual}",
                        column=name,
                    )
                )
            else:
                warnings.append(
                    ValidationIssue(
                        level="warning",
                        message=f"type mismatch for '{name}': expected {expected}, got {actual}",
                        column=name,
                    )
                )

        updated_schema = schema.model_copy(update={"columns": new_columns})
        return ValidationResult(
            errors=errors,
            warnings=warnings,
            dropped_columns=dropped,
            validated_schema=updated_schema,
        )

    def _normalize_type(self, type_str: str) -> str:
        aliases = {
            "int64": "int",
            "bigint": "int",
            "integer": "int",
            "float64": "float",
            "double": "float",
            "bool": "bool",
            "boolean": "bool",
            "timestamp[us]": "datetime",
            "timestamp[ns]": "datetime",
        }
        lowered = type_str.lower()
        if lowered in aliases:
            return aliases[lowered]
        try:
            arrow_type = string_to_arrow_type(type_str)
            return arrow_type_to_string(arrow_type)
        except ValueError:
            return lowered

