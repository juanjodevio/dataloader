"""DuckDB connector configuration."""

from typing import Literal, Optional

from pydantic import BaseModel, Field, model_validator

from dataloader.models.source_config import IncrementalConfig


class DuckDBConnectorConfig(BaseModel):
    """Configuration for DuckDB connector.

    Supports both reading and writing operations.
    """

    type: Literal["duckdb"] = "duckdb"

    # Database connection fields
    database: str = Field(
        default=":memory:",
        description="Database file path or ':memory:' for in-memory database",
    )
    table: str = Field(description="Table name")
    db_schema: Optional[str] = Field(default=None, description="Database schema")

    # Source-specific fields (for reading)
    incremental: Optional[IncrementalConfig] = Field(
        default=None, description="Incremental loading configuration (for reads)"
    )

    # Destination-specific fields (for writing)
    write_mode: Literal["append", "overwrite", "merge"] = Field(
        default="append", description="Write mode for destination (for writes)"
    )
    merge_keys: Optional[list[str]] = Field(
        default=None,
        description="Key columns for merge mode (required when write_mode='merge')",
    )

    @model_validator(mode="after")
    def validate_fields(self):
        """Validate merge_keys when write_mode is merge."""
        if self.write_mode == "merge" and not self.merge_keys:
            raise ValueError("merge_keys is required when write_mode is 'merge'")
        return self

