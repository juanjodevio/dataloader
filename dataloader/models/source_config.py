"""Source configuration model for recipe definitions."""

from typing import Literal, Optional

from pydantic import BaseModel, Field, model_validator


class IncrementalConfig(BaseModel):
    """Configuration for incremental loading strategy."""

    strategy: Literal["cursor"] = Field(
        description="Incremental strategy type (only 'cursor' supported in v0.1)"
    )
    cursor_column: str = Field(description="Column name to use as cursor for incremental loads")


class SourceConfig(BaseModel):
    """Configuration for data source."""

    type: str = Field(description="Source connector type (e.g., 'postgres', 's3', 'csv')")
    
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
    
    # File-based connectors
    path: Optional[str] = Field(
        default=None, description="File path (supports templates, required for file-based connectors)"
    )
    bucket: Optional[str] = Field(default=None, description="S3 bucket name (supports templates)")
    region: Optional[str] = Field(default=None, description="AWS region")
    access_key: Optional[str] = Field(default=None, description="AWS access key (supports templates)")
    secret_key: Optional[str] = Field(default=None, description="AWS secret key (supports templates)")
    
    incremental: Optional[IncrementalConfig] = Field(
        default=None, description="Incremental loading configuration"
    )

    @model_validator(mode="after")
    def validate_connector_fields(self):
        """Validate required fields based on connector type."""
        if self.type in ("postgres", "mysql", "redshift", "snowflake"):
            required = ["host", "database", "user", "table"]
            missing = [f for f in required if not getattr(self, f)]
            if missing:
                raise ValueError(
                    f"Source type '{self.type}' requires fields: {', '.join(missing)}"
                )
        elif self.type in ("s3",):
            if not self.bucket or not self.path:
                raise ValueError(f"Source type '{self.type}' requires 'bucket' and 'path' fields")
        elif self.type in ("csv", "parquet"):
            if not self.path:
                raise ValueError(f"Source type '{self.type}' requires 'path' field")
        return self

