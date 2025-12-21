"""Destination configuration model for recipe definitions."""

from typing import List, Literal, Optional

from pydantic import BaseModel, Field, model_validator


class DestinationConfig(BaseModel):
    """Configuration for data destination."""

    type: str = Field(description="Destination connector type (e.g., 'redshift', 's3', 'duckdb')")
    
    # Database connectors
    host: Optional[str] = Field(default=None, description="Database host (supports templates)")
    port: Optional[int] = Field(default=None, description="Database port")
    database: Optional[str] = Field(default=None, description="Database name (supports templates)")
    user: Optional[str] = Field(default=None, description="Database user (supports templates)")
    password: Optional[str] = Field(default=None, description="Database password (supports templates)")
    db_schema: Optional[str] = Field(default=None, description="Database schema")
    table: Optional[str] = Field(
        default=None, description="Table name (required for database connectors)"
    )
    
    # FileStore connector fields
    backend: Optional[str] = Field(
        default=None, description="Storage backend (e.g., 'local', 's3'). Can be inferred from filepath."
    )
    filepath: Optional[str] = Field(
        default=None, description="File path/pattern (supports glob, URL schemes: s3://, gs://, etc., required for filestore connector)"
    )
    format: Optional[str] = Field(
        default=None, description="File format (e.g., 'csv', 'json', 'jsonl', 'parquet')"
    )
    region: Optional[str] = Field(default=None, description="AWS region")
    access_key: Optional[str] = Field(default=None, description="AWS access key (supports templates)")
    secret_key: Optional[str] = Field(default=None, description="AWS secret key (supports templates)")
    
    write_mode: Literal["append", "overwrite", "merge"] = Field(
        default="append", description="Write mode for destination"
    )
    merge_keys: Optional[List[str]] = Field(
        default=None, description="Key columns for merge mode (required when write_mode='merge')"
    )

    @model_validator(mode="after")
    def validate_fields(self):
        """Validate connector fields and merge_keys."""
        if self.type in ("redshift", "snowflake", "postgres"):
            required = ["host", "database", "user", "table"]
            missing = [f for f in required if not getattr(self, f)]
            if missing:
                raise ValueError(
                    f"Destination type '{self.type}' requires fields: {', '.join(missing)}"
                )
        elif self.type == "duckdb":
            # DuckDB uses database as file path, doesn't need host/user
            required = ["database", "table"]
            missing = [f for f in required if not getattr(self, f)]
            if missing:
                raise ValueError(
                    f"Destination type 'duckdb' requires fields: {', '.join(missing)}"
                )
        elif self.type == "filestore":
            if not self.filepath:
                raise ValueError("Destination type 'filestore' requires 'filepath' field")
            if not self.format:
                raise ValueError("Destination type 'filestore' requires 'format' field")

        if self.write_mode == "merge" and not self.merge_keys:
            raise ValueError("merge_keys is required when write_mode is 'merge'")

        return self

