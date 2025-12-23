"""Source configuration model for recipe definitions."""

from typing import Literal, Optional

from pydantic import BaseModel, Field, SecretStr, model_validator


class IncrementalConfig(BaseModel):
    """Configuration for incremental loading strategy."""

    strategy: Literal["cursor"] = Field(
        description="Incremental strategy type (only 'cursor' supported in v0.1)"
    )
    cursor_column: str = Field(
        description="Column name to use as cursor for incremental loads"
    )


class SourceConfig(BaseModel):
    """Configuration for data source."""

    type: str = Field(
        description="Source connector type (e.g., 'postgres', 'duckdb', 'filestore')"
    )

    # Database connectors
    host: Optional[str] = Field(
        default=None, description="Database host (supports templates)"
    )
    port: Optional[int] = Field(default=None, description="Database port")
    database: Optional[str] = Field(
        default=None, description="Database name (supports templates)"
    )
    user: Optional[str] = Field(
        default=None, description="Database user (supports templates)"
    )
    password: Optional[SecretStr] = Field(
        default=None, description="Database password (supports templates)"
    )
    db_schema: Optional[str] = Field(default=None, description="Database schema")
    table: Optional[str] = Field(
        default=None, description="Table name (required for database connectors)"
    )

    # FileStore connector fields
    backend: Optional[str] = Field(
        default=None,
        description="Storage backend (e.g., 'local', 's3'). Can be inferred from filepath.",
    )
    filepath: Optional[str] = Field(
        default=None,
        description="File path/pattern (supports glob, URL schemes: s3://, gs://, etc., required for filestore connector)",
    )
    format: Optional[str] = Field(
        default=None,
        description="File format (e.g., 'csv', 'json', 'jsonl', 'parquet')",
    )
    region: Optional[str] = Field(default=None, description="AWS region")
    access_key: Optional[str] = Field(
        default=None, description="AWS access key (supports templates)"
    )
    secret_key: Optional[SecretStr] = Field(
        default=None, description="AWS secret key (supports templates)"
    )

    # API connector fields
    base_url: Optional[str] = Field(
        default=None,
        description="Base URL of the API (supports templates, required for API connector)",
    )
    endpoint: Optional[str] = Field(
        default=None,
        description="Relative endpoint path (e.g., '/api/search', required for API connector)",
    )
    params: Optional[dict] = Field(
        default=None, description="Query string parameters (supports templates)"
    )
    headers: Optional[dict[str, str]] = Field(
        default=None, description="HTTP headers (supports templates)"
    )
    auth_type: Optional[Literal["none", "bearer", "basic"]] = Field(
        default=None, description="Authentication type"
    )
    auth_token: Optional[SecretStr] = Field(
        default=None, description="Token for bearer auth (supports templates)"
    )
    auth_username: Optional[str] = Field(
        default=None, description="Username for basic auth (supports templates)"
    )
    auth_password: Optional[SecretStr] = Field(
        default=None, description="Password for basic auth (supports templates)"
    )
    pagination_type: Optional[Literal["page", "offset", "cursor"]] = Field(
        default=None, description="Pagination strategy"
    )
    page_param: Optional[str] = Field(
        default=None, description="Name of the page parameter"
    )
    limit_param: Optional[str] = Field(
        default=None, description="Name of the limit parameter"
    )
    page_size: Optional[int] = Field(default=None, description="Default page size")
    data_path: Optional[str] = Field(
        default=None,
        description="JSONPath to extract data array (e.g., 'data', 'results.items')",
    )
    total_path: Optional[str] = Field(
        default=None,
        description="JSONPath to extract total count (e.g., 'total', 'pagination.total')",
    )
    timeout: Optional[int] = Field(
        default=None, description="Request timeout in seconds"
    )
    max_retries: Optional[int] = Field(
        default=None, description="Maximum number of retries"
    )
    retry_delay: Optional[float] = Field(
        default=None, description="Initial delay between retries in seconds"
    )
    backoff_rate: Optional[float] = Field(
        default=None, description="Exponential backoff multiplier for retry_delay"
    )

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
        elif self.type == "duckdb":
            if not self.table:
                raise ValueError("Source type 'duckdb' requires 'table' field")
            # database defaults to :memory: so it's optional
        elif self.type == "filestore":
            if not self.filepath:
                raise ValueError("Source type 'filestore' requires 'filepath' field")
            if not self.format:
                raise ValueError("Source type 'filestore' requires 'format' field")
        elif self.type == "api":
            if not self.base_url:
                raise ValueError("Source type 'api' requires 'base_url' field")
            if not self.endpoint:
                raise ValueError("Source type 'api' requires 'endpoint' field")
        return self

    def _infer_backend(self) -> str:
        """Infer storage backend from filepath."""
        if not self.filepath:
            return "local"
        # Check for URL schemes
        if self.filepath.startswith("s3://"):
            return "s3"
        elif self.filepath.startswith("gs://"):
            return "gcs"
        elif self.filepath.startswith("az://") or self.filepath.startswith("abfss://"):
            return "azure"
        return "local"
