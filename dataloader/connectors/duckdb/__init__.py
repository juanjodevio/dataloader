"""DuckDB connector module."""

from dataloader.connectors.duckdb.config import DuckDBConnectorConfig
from dataloader.connectors.duckdb.connector import (
    DuckDBConnector,
    create_duckdb_connector,
)

__all__ = [
    "DuckDBConnector",
    "DuckDBConnectorConfig",
    "create_duckdb_connector",
]
