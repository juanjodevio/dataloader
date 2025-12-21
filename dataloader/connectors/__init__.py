"""Connector protocols and registry for data sources and destinations.

This module exposes:
- Source, Destination: Protocols defining connector interfaces
- Registry functions: register_source, register_destination, get_source, get_destination
- Utility functions: list_source_types, list_destination_types, clear_registries
- Source implementations: PostgresSource, CSVSource, S3Source
- Destination implementations: DuckDBDestination, S3Destination
"""

from dataloader.connectors.base import Destination, Source

# Registry must be imported first (connector modules use decorators on import)
from dataloader.connectors.registry import (
    DestinationFactory,
    SourceFactory,
    clear_registries,
    get_connector,
    get_destination,
    get_source,
    list_connector_types,
    list_destination_types,
    list_source_types,
    register_connector,
    register_destination,
    register_source,
)

# Source modules register themselves via @register_source decorator on import
from dataloader.connectors.csv.source import CSVSource, create_csv_source
from dataloader.connectors.postgres.source import PostgresSource, create_postgres_source
from dataloader.connectors.s3.source import S3Source, create_s3_source

# Unified connectors (register themselves via @register_connector decorator on import)
from dataloader.connectors.csv.connector import CSVConnector, create_csv_connector
from dataloader.connectors.postgres.connector import (
    PostgresConnector,
    create_postgres_connector,
)

# Destination modules register themselves via @register_destination decorator on import
from dataloader.connectors.duckdb.destination import (
    DuckDBDestination,
    create_duckdb_destination,
)
from dataloader.connectors.s3.destination import S3Destination, create_s3_destination

# Unified connector (registers itself via @register_connector decorator on import)
from dataloader.connectors.s3.connector import S3Connector, create_s3_connector


def reregister_builtins() -> None:
    """Re-register built-in connectors after registry is cleared.

    This is intended for tests that call clear_registries() but need
    the built-in connectors available afterwards.
    """
    # Unified connectors (preferred)
    current_connectors = list_connector_types()
    if "csv" not in current_connectors:
        register_connector("csv", create_csv_connector)
    if "postgres" not in current_connectors:
        register_connector("postgres", create_postgres_connector)
    if "s3" not in current_connectors:
        register_connector("s3", create_s3_connector)

    # Legacy sources (for backward compatibility)
    current_sources = list_source_types()
    if "postgres" not in current_sources:
        register_source("postgres", create_postgres_source)
    if "csv" not in current_sources:
        register_source("csv", create_csv_source)
    if "s3" not in current_sources:
        register_source("s3", create_s3_source)

    # Legacy destinations (for backward compatibility)
    current_destinations = list_destination_types()
    if "duckdb" not in current_destinations:
        register_destination("duckdb", create_duckdb_destination)
    if "s3" not in current_destinations:
        register_destination("s3", create_s3_destination)


__all__ = [
    # Protocols
    "Source",
    "Destination",
    # Factory types
    "SourceFactory",
    "DestinationFactory",
    # Registration functions
    "register_connector",
    "register_source",
    "register_destination",
    "reregister_builtins",
    # Retrieval functions
    "get_connector",
    "get_source",
    "get_destination",
    # Utility functions
    "list_connector_types",
    "list_source_types",
    "list_destination_types",
    "clear_registries",
    # Source implementations
    "PostgresSource",
    "CSVSource",
    "S3Source",
    # Destination implementations
    "DuckDBDestination",
    "S3Destination",
    # Unified connector implementations
    "CSVConnector",
    "PostgresConnector",
    "S3Connector",
    # Source factory functions
    "create_postgres_source",
    "create_csv_source",
    "create_s3_source",
    # Destination factory functions
    "create_duckdb_destination",
    "create_s3_destination",
    # Unified connector factory functions
    "create_csv_connector",
    "create_postgres_connector",
    "create_s3_connector",
]

