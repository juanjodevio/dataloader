"""Unified connector configuration models for recipe definitions.

This module provides a base protocol and re-exports all connector-specific
configuration classes. Each connector config is co-located with its connector
implementation for better cohesion and maintainability.
"""

from typing import TYPE_CHECKING, Protocol, Union

if TYPE_CHECKING:
    from dataloader.connectors.api.config import ApiConnectorConfig
    from dataloader.connectors.duckdb.config import DuckDBConnectorConfig
    from dataloader.connectors.filestore.config import (
        FileStoreConfigType,
        LocalFileStoreConfig,
        S3FileStoreConfig,
    )
    from dataloader.connectors.postgres.config import PostgresConnectorConfig
    from dataloader.models.destination_config import DestinationConfig
    from dataloader.models.source_config import SourceConfig


class ConnectorConfig(Protocol):
    """Base protocol for connector configurations.

    All connector config classes must have a 'type' field that identifies
    the connector type. This allows type-safe handling of different configs.
    """

    type: str


# Import connector-specific configs from their respective modules
# These imports are safe because config modules don't import connectors
from dataloader.connectors.api.config import ApiConnectorConfig
from dataloader.connectors.duckdb.config import DuckDBConnectorConfig
from dataloader.connectors.filestore.config import (
    FileStoreConfigType,
    LocalFileStoreConfig,
    S3FileStoreConfig,
)
from dataloader.connectors.postgres.config import PostgresConnectorConfig

# Union type for all connector configs
# This is used by the registry and recipe loading
ConnectorConfigType = Union[
    ApiConnectorConfig,
    PostgresConnectorConfig,
    DuckDBConnectorConfig,
    FileStoreConfigType,  # Includes S3FileStoreConfig, LocalFileStoreConfig
]

# Re-export ConnectorConfigUnion from registry for convenience
from dataloader.connectors.registry import ConnectorConfigUnion

# Re-export all config classes for convenience
__all__ = [
    "ConnectorConfig",
    "ConnectorConfigType",
    "ConnectorConfigUnion",
    "ApiConnectorConfig",
    "PostgresConnectorConfig",
    "DuckDBConnectorConfig",
    "FileStoreConfigType",
    "S3FileStoreConfig",
    "LocalFileStoreConfig",
]
