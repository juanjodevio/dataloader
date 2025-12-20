"""Postgres connector module."""

from dataloader.connectors.postgres.config import PostgresConnectorConfig
from dataloader.connectors.postgres.connector import (
    PostgresConnector,
    create_postgres_connector,
)
from dataloader.connectors.postgres.source import PostgresSource

__all__ = [
    "PostgresConnector",
    "PostgresConnectorConfig",
    "create_postgres_connector",
    # Legacy exports for backward compatibility
    "PostgresSource",
]

