"""S3 connector configuration."""

from typing import Literal, Optional

from pydantic import BaseModel, Field

from dataloader.models.source_config import IncrementalConfig


class S3ConnectorConfig(BaseModel):
    """Configuration for S3 connector.

    Supports both reading and writing operations.
    """

    type: Literal["s3"] = "s3"

    # S3 connection fields
    bucket: str = Field(description="S3 bucket name (supports templates)")
    path: str = Field(description="S3 object path/prefix (supports templates)")
    region: Optional[str] = Field(default=None, description="AWS region")
    access_key: Optional[str] = Field(
        default=None, description="AWS access key (supports templates)"
    )
    secret_key: Optional[str] = Field(
        default=None, description="AWS secret key (supports templates)"
    )

    # Source-specific fields (for reading)
    incremental: Optional[IncrementalConfig] = Field(
        default=None, description="Incremental loading configuration (for reads)"
    )

    # Destination-specific fields (for writing)
    write_mode: Literal["append", "overwrite"] = Field(
        default="append", description="Write mode for destination (for writes)"
    )

