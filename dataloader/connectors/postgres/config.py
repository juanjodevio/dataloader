"""PostgreSQL connector configuration."""

from typing import Literal, Optional

from pydantic import BaseModel, Field, model_validator

from dataloader.models.source_config import IncrementalConfig


class PostgresConnectorConfig(BaseModel):
    """Configuration for PostgreSQL connector.

    Supports both reading and writing operations.
    """

    type: Literal["postgres"] = "postgres"

    # Database connection fields
    host: str = Field(description="Database host (supports templates)")
    port: Optional[int] = Field(default=5432, description="Database port")
    database: str = Field(description="Database name (supports templates)")
    user: str = Field(description="Database user (supports templates)")
    password: Optional[str] = Field(
        default=None, description="Database password (supports templates)"
    )
    db_schema: Optional[str] = Field(default="public", description="Database schema")
    table: str = Field(description="Table name")

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

