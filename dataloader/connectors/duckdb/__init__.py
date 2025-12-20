"""DuckDB connector module."""

from dataloader.connectors.duckdb.config import DuckDBConnectorConfig
from dataloader.connectors.duckdb.destination import (
    DuckDBDestination,
    create_duckdb_destination,
)

__all__ = ["DuckDBDestination", "DuckDBConnectorConfig", "create_duckdb_destination"]

