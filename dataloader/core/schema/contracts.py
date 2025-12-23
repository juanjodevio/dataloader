"""Schema contract definitions (Phase 3, dlt-inspired)."""

from __future__ import annotations

from enum import Enum
from typing import Dict

from pydantic import BaseModel, Field


class ContractMode(str, Enum):
    EVOLVE = "evolve"
    FREEZE = "freeze"
    DISCARD_ROWS = "discard_rows"
    DISCARD_COLUMNS = "discard_columns"


class SchemaContracts(BaseModel):
    default: ContractMode = ContractMode.EVOLVE
    tables: Dict[str, ContractMode] = Field(default_factory=dict)
    columns: Dict[str, ContractMode] = Field(default_factory=dict)
    data_types: Dict[str, ContractMode] = Field(default_factory=dict)

    def table_mode(self, table_name: str) -> ContractMode:
        return self.tables.get(table_name, self.default)

    def column_mode(
        self, column_name: str, data_type: str | None = None
    ) -> ContractMode:
        if column_name in self.columns:
            return self.columns[column_name]
        if data_type and data_type in self.data_types:
            return self.data_types[data_type]
        return self.default

    def data_type_mode(self, data_type: str) -> ContractMode:
        return self.data_types.get(data_type, self.default)
