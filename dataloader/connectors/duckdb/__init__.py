"""DuckDB connector module."""

from dataloader.connectors.duckdb.destination import (
    DuckDBDestination,
    create_duckdb_destination,
)

__all__ = ["DuckDBDestination", "create_duckdb_destination"]

