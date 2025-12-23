"""Schema configuration model for recipes (Phase 5)."""

from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel, Field

from dataloader.core.schema import (
    Column,
    ContractMode,
    EvolutionPolicy,
    Schema,
    SchemaContracts,
    SchemaMode,
)


class ColumnConfig(BaseModel):
    name: str
    type: str
    nullable: bool = True
    primary_key: bool = False
    unique: bool = False
    metadata: dict = Field(default_factory=dict)

    def to_column(self) -> Column:
        return Column(
            name=self.name,
            type=self.type,
            nullable=self.nullable,
            primary_key=self.primary_key,
            unique=self.unique,
            metadata=self.metadata,
        )


class SchemaConfig(BaseModel):
    mode: SchemaMode = SchemaMode.INFER
    reflection_level: str = "deep"  # none | shallow | deep
    contracts: SchemaContracts = Field(default_factory=SchemaContracts)
    columns: List[ColumnConfig] = Field(default_factory=list)
    evolution: EvolutionPolicy = Field(default_factory=EvolutionPolicy)
    version: Optional[str] = None
    metadata: dict = Field(default_factory=dict)

    def to_schema(self) -> Schema:
        return Schema(
            columns=[col.to_column() for col in self.columns],
            mode=self.mode,
            evolution=self.evolution,
            version=self.version,
            metadata=self.metadata,
        )

    @classmethod
    def default_contracts(cls) -> SchemaContracts:
        return SchemaContracts()


__all__ = ["SchemaConfig", "ColumnConfig", "ContractMode"]
