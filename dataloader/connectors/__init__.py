"""Connector protocols and registry for data sources and destinations.

This module exposes:
- Source, Destination: Protocols defining connector interfaces
- Registry functions: register_source, register_destination, get_source, get_destination
- Utility functions: list_source_types, list_destination_types, clear_registries
- Source implementations: PostgresSource, CSVSource, S3Source
"""

from dataloader.connectors.base import Destination, Source

# Registry must be imported first (source modules use @register_source decorator)
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


def reregister_builtins() -> None:
    """Re-register built-in connectors after registry is cleared.

    This is intended for tests that call clear_registries() but need
    the built-in connectors available afterwards.
    """
    current = list_source_types()
    if "postgres" not in current:
        register_source("postgres", create_postgres_source)
    if "csv" not in current:
        register_source("csv", create_csv_source)
    if "s3" not in current:
        register_source("s3", create_s3_source)


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
    # Factory functions
    "create_postgres_source",
    "create_csv_source",
    "create_s3_source",
]

