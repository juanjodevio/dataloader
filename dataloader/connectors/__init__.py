"""Connector protocols and registry for data sources and destinations.

This module exposes:
- Source, Destination: Protocols defining connector interfaces
- Registry functions: register_source, register_destination, get_source, get_destination
- Utility functions: list_source_types, list_destination_types, clear_registries
- Source implementations: PostgresSource, CSVSource, S3Source
"""

from dataloader.connectors.base import Destination, Source
from dataloader.connectors.csv.source import CSVSource, create_csv_source
from dataloader.connectors.postgres.source import PostgresSource, create_postgres_source
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
from dataloader.connectors.s3.source import S3Source, create_s3_source


def register_builtin_connectors() -> None:
    """Register all built-in source and destination connectors.

    This function is idempotent - calling it multiple times is safe.
    It checks if connectors are already registered before registering.
    """
    current_sources = list_source_types()

    if "postgres" not in current_sources:
        register_source("postgres", create_postgres_source)
    if "csv" not in current_sources:
        register_source("csv", create_csv_source)
    if "s3" not in current_sources:
        register_source("s3", create_s3_source)


# Auto-register built-in connectors on import
register_builtin_connectors()

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
    "register_builtin_connectors",
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
]

