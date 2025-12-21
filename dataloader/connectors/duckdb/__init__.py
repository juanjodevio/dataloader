"""DuckDB connector module."""

from dataloader.connectors.duckdb.config import DuckDBConnectorConfig
from dataloader.connectors.duckdb.connector import (
    DuckDBConnector,
    create_duckdb_connector,
)
from dataloader.connectors.duckdb.destination import (
    DuckDBDestination,
    create_duckdb_destination,
)

__all__ = [
    "DuckDBConnector",
    "DuckDBConnectorConfig",
    "create_duckdb_connector",
    # Legacy exports (deprecated)
    "DuckDBDestination",
    "create_duckdb_destination",
]

