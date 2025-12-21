"""FileStore connector configuration.

This module defines configuration classes for the unified FileStore connector,
which supports multiple storage backends (S3, local filesystem, Azure, GCS, etc.)
using fsspec for abstraction.
"""

from typing import Literal, Optional, Union

from pydantic import BaseModel, Field, model_validator

from dataloader.models.source_config import IncrementalConfig


class FileStoreConnectorConfig(BaseModel):
    """Base configuration for FileStore connector.

    This is a base class that should not be used directly.
    Use specific config classes like S3FileStoreConfig or LocalFileStoreConfig.
    """

    type: Literal["filestore"] = "filestore"
    path: str = Field(
        description="File path or prefix (supports templates and URL formats)"
    )

    # Source-specific fields (for reading)
    incremental: Optional[IncrementalConfig] = Field(
        default=None, description="Incremental loading configuration (for reads)"
    )

    # Destination-specific fields (for writing)
    write_mode: Literal["append", "overwrite"] = Field(
        default="append", description="Write mode for destination (for writes)"
    )

    # Format configuration
    format: Literal["csv", "json", "jsonl", "parquet"] = Field(
        default="csv", description="File format to read/write"
    )


class S3FileStoreConfig(FileStoreConnectorConfig):
    """Configuration for S3 FileStore backend."""

    type: Literal["filestore"] = "filestore"
    backend: Literal["s3"] = Field(default="s3", description="Storage backend type")

    # S3-specific fields
    bucket: str = Field(description="S3 bucket name (supports templates)")
    region: Optional[str] = Field(default=None, description="AWS region")
    access_key: Optional[str] = Field(
        default=None, description="AWS access key (supports templates)"
    )
    secret_key: Optional[str] = Field(
        default=None, description="AWS secret key (supports templates)"
    )

    @model_validator(mode="after")
    def validate_s3_path(self):
        """Validate that path is provided for S3."""
        if not self.path:
            raise ValueError("path is required for S3 FileStore")
        return self


class LocalFileStoreConfig(FileStoreConnectorConfig):
    """Configuration for local filesystem FileStore backend."""

    type: Literal["filestore"] = "filestore"
    backend: Literal["local"] = Field(
        default="local", description="Storage backend type"
    )

    @model_validator(mode="after")
    def validate_local_path(self):
        """Validate that path is provided for local filesystem."""
        if not self.path:
            raise ValueError("path is required for local FileStore")
        return self


# Union type for all FileStore configs
FileStoreConfigType = Union[S3FileStoreConfig, LocalFileStoreConfig]
