"""Postgres connector module."""

from dataloader.connectors.postgres.config import PostgresConnectorConfig
from dataloader.connectors.postgres.source import PostgresSource

__all__ = ["PostgresSource", "PostgresConnectorConfig"]

