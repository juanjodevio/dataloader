"""Unified connector configuration models for recipe definitions.

This module provides a base protocol and re-exports all connector-specific
configuration classes. Each connector config is co-located with its connector
implementation for better cohesion and maintainability.
"""

from typing import Protocol, Union

# Import connector-specific configs from their respective modules
from dataloader.connectors.csv.config import CSVConnectorConfig
from dataloader.connectors.duckdb.config import DuckDBConnectorConfig
from dataloader.connectors.postgres.config import PostgresConnectorConfig
from dataloader.connectors.s3.config import S3ConnectorConfig


class ConnectorConfig(Protocol):
    """Base protocol for connector configurations.

    All connector config classes must have a 'type' field that identifies
    the connector type. This allows type-safe handling of different configs.
    """

    type: str


# Union type for all connector configs
# This is used by the registry and recipe loading
ConnectorConfigType = Union[
    PostgresConnectorConfig,
    S3ConnectorConfig,
    CSVConnectorConfig,
    DuckDBConnectorConfig,
]

# Re-export all config classes for convenience
__all__ = [
    "ConnectorConfig",
    "ConnectorConfigType",
    "PostgresConnectorConfig",
    "S3ConnectorConfig",
    "CSVConnectorConfig",
    "DuckDBConnectorConfig",
]
