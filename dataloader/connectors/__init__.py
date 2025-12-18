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
    get_destination,
    get_source,
    list_destination_types,
    list_source_types,
    register_destination,
    register_source,
)

# Source modules register themselves via @register_source decorator on import
from dataloader.connectors.csv.source import CSVSource, create_csv_source
from dataloader.connectors.postgres.source import PostgresSource, create_postgres_source
from dataloader.connectors.s3.source import S3Source, create_s3_source

# Destination modules register themselves via @register_destination decorator on import
from dataloader.connectors.duckdb.destination import (
    DuckDBDestination,
    create_duckdb_destination,
)
from dataloader.connectors.s3.destination import S3Destination, create_s3_destination


def reregister_builtins() -> None:
    """Re-register built-in connectors after registry is cleared.

    This is intended for tests that call clear_registries() but need
    the built-in connectors available afterwards.
    """
    # Sources
    current_sources = list_source_types()
    if "postgres" not in current_sources:
        register_source("postgres", create_postgres_source)
    if "csv" not in current_sources:
        register_source("csv", create_csv_source)
    if "s3" not in current_sources:
        register_source("s3", create_s3_source)

    # Destinations
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
    "register_source",
    "register_destination",
    "reregister_builtins",
    # Retrieval functions
    "get_source",
    "get_destination",
    # Utility functions
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
    # Source factory functions
    "create_postgres_source",
    "create_csv_source",
    "create_s3_source",
    # Destination factory functions
    "create_duckdb_destination",
    "create_s3_destination",
]

