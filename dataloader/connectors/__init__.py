"""Connector protocols and registry for unified data connectors.

This module exposes:
- Connector: Unified protocol for connectors that can read and/or write data
- Registry functions: register_connector, get_connector, list_connector_types
- Connector implementations: PostgresConnector, DuckDBConnector, FileStoreConnector
"""

from dataloader.connectors.base import Connector

# Registry must be imported first (connector modules use decorators on import)
from dataloader.connectors.registry import (
    clear_registries,
    get_connector,
    list_connector_types,
    register_connector,
)

# Unified connectors (register themselves via @register_connector decorator on import)
# CSV connector removed - use FileStore connector with format="csv" instead
# from dataloader.connectors.csv.connector import CSVConnector, create_csv_connector
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

# S3 connector removed - use FileStore connector with backend="s3" instead
# from dataloader.connectors.s3.connector import S3Connector, create_s3_connector


def reregister_builtins() -> None:
    """Re-register built-in connectors after registry is cleared.

    This is intended for tests that call clear_registries() but need
    the built-in connectors available afterwards.
    """
    # Unified connectors (preferred)
    current_connectors = list_connector_types()
    # CSV connector removed - use FileStore connector with format="csv" instead
    # if "csv" not in current_connectors:
    #     register_connector("csv", create_csv_connector)
    if "duckdb" not in current_connectors:
        register_connector("duckdb", create_duckdb_connector)
    if "filestore" not in current_connectors:
        register_connector("filestore", create_filestore_connector)
    if "postgres" not in current_connectors:
        register_connector("postgres", create_postgres_connector)
    # S3 connector removed - use FileStore connector with backend="s3" instead
    # if "s3" not in current_connectors:
    #     register_connector("s3", create_s3_connector)


__all__ = [
    # Protocols
    "Connector",
    # Registration functions
    "register_connector",  # Primary registration function
    "reregister_builtins",
    # Retrieval functions
    "get_connector",  # Primary retrieval function
    # Utility functions
    "list_connector_types",  # Primary listing function
    "clear_registries",
    # Unified connector implementations
    # "CSVConnector",  # Removed - use FileStore connector with format="csv" instead
    "DuckDBConnector",
    "FileStoreConnector",
    "PostgresConnector",
    # "S3Connector",  # Removed - use FileStore connector with backend="s3" instead
    # Unified connector factory functions
    # "create_csv_connector",  # Removed - use FileStore connector with format="csv" instead
    "create_duckdb_connector",
    "create_filestore_connector",
    "create_postgres_connector",
    # "create_s3_connector",  # Removed - use FileStore connector with backend="s3" instead
]

