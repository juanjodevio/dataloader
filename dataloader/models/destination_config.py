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
    
    # File-based connectors
    path: Optional[str] = Field(
        default=None, description="File path (supports templates, required for file-based connectors)"
    )
    bucket: Optional[str] = Field(default=None, description="S3 bucket name (supports templates)")
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
        if self.type in ("redshift", "snowflake", "duckdb", "postgres"):
            required = ["host", "database", "user", "table"]
            missing = [f for f in required if not getattr(self, f)]
            if missing:
                raise ValueError(
                    f"Destination type '{self.type}' requires fields: {', '.join(missing)}"
                )
        elif self.type in ("s3",):
            if not self.bucket or not self.path:
                raise ValueError(f"Destination type '{self.type}' requires 'bucket' and 'path' fields")
        elif self.type in ("csv", "parquet"):
            if not self.path:
                raise ValueError(f"Destination type '{self.type}' requires 'path' field")

        if self.write_mode == "merge" and not self.merge_keys:
            raise ValueError("merge_keys is required when write_mode is 'merge'")

        return self

