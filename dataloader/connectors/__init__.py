"""Connector protocols and registry for unified data connectors.

This module exposes:
- Connector: Unified protocol for connectors that can read and/or write data
- Registry functions: register_connector, get_connector, list_connector_types (primary)
- Legacy functions: register_source, register_destination, get_source, get_destination (deprecated)
- Unified connector implementations: PostgresConnector, CSVConnector, S3Connector, DuckDBConnector, FileStoreConnector
- Legacy implementations: PostgresSource, CSVSource, S3Source, DuckDBDestination, S3Destination (deprecated)
"""

from dataloader.connectors.base import Connector, Destination, Source

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
from dataloader.connectors.duckdb.connector import (
    DuckDBConnector,
    create_duckdb_connector,
)
from dataloader.connectors.filestore.connector import (
    FileStoreConnector,
    create_filestore_connector,
)
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
    if "duckdb" not in current_connectors:
        register_connector("duckdb", create_duckdb_connector)
    if "filestore" not in current_connectors:
        register_connector("filestore", create_filestore_connector)
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
    # Protocols (unified first, legacy for backward compatibility)
    "Connector",  # Primary protocol
    "Source",  # Deprecated, use Connector
    "Destination",  # Deprecated, use Connector
    # Factory types
    "SourceFactory",  # Deprecated
    "DestinationFactory",  # Deprecated
    # Registration functions (unified first)
    "register_connector",  # Primary registration function
    "reregister_builtins",
    "register_source",  # Deprecated, use register_connector
    "register_destination",  # Deprecated, use register_connector
    # Retrieval functions (unified first)
    "get_connector",  # Primary retrieval function
    "get_source",  # Deprecated, use get_connector
    "get_destination",  # Deprecated, use get_connector
    # Utility functions (unified first)
    "list_connector_types",  # Primary listing function
    "list_source_types",  # Deprecated, use list_connector_types
    "list_destination_types",  # Deprecated, use list_connector_types
    "clear_registries",
    # Unified connector implementations (primary)
    "CSVConnector",
    "DuckDBConnector",
    "FileStoreConnector",
    "PostgresConnector",
    "S3Connector",
    # Unified connector factory functions (primary)
    "create_csv_connector",
    "create_duckdb_connector",
    "create_filestore_connector",
    "create_postgres_connector",
    "create_s3_connector",
    # Legacy source implementations (deprecated)
    "PostgresSource",
    "CSVSource",
    "S3Source",
    # Legacy destination implementations (deprecated)
    "DuckDBDestination",
    "S3Destination",
    # Legacy source factory functions (deprecated)
    "create_postgres_source",
    "create_csv_source",
    "create_s3_source",
    # Legacy destination factory functions (deprecated)
    "create_duckdb_destination",
    "create_s3_destination",
]

