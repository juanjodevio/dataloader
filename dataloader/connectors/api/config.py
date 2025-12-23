"""API connector configuration."""

from typing import Any, Literal, Optional

from pydantic import BaseModel, Field, SecretStr, model_validator

from dataloader.models.source_config import IncrementalConfig


class ApiConnectorConfig(BaseModel):
    """Configuration for API connector.

    Supports reading data from REST APIs with pagination, authentication,
    and error handling.
    """

    type: Literal["api"] = "api"

    # API connection fields
    base_url: str = Field(description="Base URL of the API (supports templates)")
    endpoint: str = Field(description="Relative endpoint path (e.g., '/api/search')")
    params: Optional[dict[str, Any]] = Field(
        default=None, description="Query string parameters (supports templates)"
    )
    headers: Optional[dict[str, str]] = Field(
        default=None, description="HTTP headers (supports templates)"
    )

    # Authentication
    auth_type: Literal["none", "bearer", "basic"] = Field(
        default="none", description="Authentication type"
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

    # Pagination
    pagination_type: Literal["page", "offset", "cursor"] = Field(
        default="page", description="Pagination strategy"
    )
    page_param: str = Field(default="page", description="Name of the page parameter")
    limit_param: Optional[str] = Field(
        default=None, description="Name of the limit parameter (optional)"
    )
    page_size: int = Field(default=100, description="Default page size", ge=1)

    # Response parsing
    data_path: Optional[str] = Field(
        default=None,
        description="JSONPath to extract data array (e.g., 'data', 'results.items')",
    )
    total_path: Optional[str] = Field(
        default=None,
        description="JSONPath to extract total count (e.g., 'total', 'pagination.total')",
    )

    # Error handling
    timeout: int = Field(default=30, description="Request timeout in seconds", ge=1)
    max_retries: int = Field(default=3, description="Maximum number of retries", ge=0)
    retry_delay: float = Field(
        default=1.0, description="Initial delay between retries in seconds", ge=0.0
    )
    backoff_rate: float = Field(
        default=2.0,
        description="Exponential backoff multiplier for retry_delay (e.g., 1.0s, 2.0s, 4.0s, 8.0s)",
        ge=1.0,
    )

    # Incremental loading
    incremental: Optional[IncrementalConfig] = Field(
        default=None, description="Incremental loading configuration"
    )

    @model_validator(mode="after")
    def validate_auth_fields(self):
        """Validate authentication fields based on auth_type."""
        if self.auth_type == "bearer" and not self.auth_token:
            raise ValueError("auth_token is required when auth_type is 'bearer'")
        if self.auth_type == "basic":
            if not self.auth_username:
                raise ValueError("auth_username is required when auth_type is 'basic'")
            if not self.auth_password:
                raise ValueError("auth_password is required when auth_type is 'basic'")
        return self
