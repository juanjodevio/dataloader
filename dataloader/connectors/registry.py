"""Connector registry for managing source and destination factories.

This module provides a registry pattern for connector implementations,
allowing dynamic registration and retrieval of source/destination factories.
"""

from typing import Callable

from dataloader.connectors.base import Destination, Source
from dataloader.core.exceptions import ConnectorError
from dataloader.models.destination_config import DestinationConfig
from dataloader.models.source_config import SourceConfig

# Type aliases for factory signatures
SourceFactory = Callable[[SourceConfig, dict], Source]
DestinationFactory = Callable[[DestinationConfig, dict], Destination]

# Global registries
_source_registry: dict[str, SourceFactory] = {}
_destination_registry: dict[str, DestinationFactory] = {}


def register_source(connector_type: str, factory: SourceFactory) -> None:
    """Register a source connector factory.

    Args:
        connector_type: Unique identifier for the connector (e.g., 'postgres', 'csv').
        factory: Factory function that creates Source instances.
                 Signature: (config: SourceConfig, connection: dict) -> Source

    Raises:
        ConnectorError: If a connector with the same type is already registered.

    Example:
        def create_postgres_source(config: SourceConfig, connection: dict) -> Source:
            return PostgresSource(config, connection)

        register_source("postgres", create_postgres_source)
    """
    if connector_type in _source_registry:
        raise ConnectorError(
            f"Source connector '{connector_type}' is already registered",
            context={"connector_type": connector_type},
        )
    _source_registry[connector_type] = factory


def register_destination(connector_type: str, factory: DestinationFactory) -> None:
    """Register a destination connector factory.

    Args:
        connector_type: Unique identifier for the connector (e.g., 'duckdb', 's3').
        factory: Factory function that creates Destination instances.
                 Signature: (config: DestinationConfig, connection: dict) -> Destination

    Raises:
        ConnectorError: If a connector with the same type is already registered.

    Example:
        def create_duckdb_destination(config: DestinationConfig, connection: dict) -> Destination:
            return DuckDBDestination(config, connection)

        register_destination("duckdb", create_duckdb_destination)
    """
    if connector_type in _destination_registry:
        raise ConnectorError(
            f"Destination connector '{connector_type}' is already registered",
            context={"connector_type": connector_type},
        )
    _destination_registry[connector_type] = factory


def get_source(
    connector_type: str,
    config: SourceConfig,
    connection: dict,
) -> Source:
    """Create a source connector instance using the registered factory.

    Args:
        connector_type: The connector type to instantiate.
        config: Source configuration from the recipe.
        connection: Connection parameters (host, port, credentials, etc.).

    Returns:
        A Source instance configured for reading.

    Raises:
        ConnectorError: If the connector type is not registered.
    """
    factory = _source_registry.get(connector_type)
    if factory is None:
        available = ", ".join(sorted(_source_registry.keys())) or "(none)"
        raise ConnectorError(
            f"Unknown source connector type: '{connector_type}'",
            context={"connector_type": connector_type, "available_types": available},
        )
    return factory(config, connection)


def get_destination(
    connector_type: str,
    config: DestinationConfig,
    connection: dict,
) -> Destination:
    """Create a destination connector instance using the registered factory.

    Args:
        connector_type: The connector type to instantiate.
        config: Destination configuration from the recipe.
        connection: Connection parameters (host, port, credentials, etc.).

    Returns:
        A Destination instance configured for writing.

    Raises:
        ConnectorError: If the connector type is not registered.
    """
    factory = _destination_registry.get(connector_type)
    if factory is None:
        available = ", ".join(sorted(_destination_registry.keys())) or "(none)"
        raise ConnectorError(
            f"Unknown destination connector type: '{connector_type}'",
            context={"connector_type": connector_type, "available_types": available},
        )
    return factory(config, connection)


def list_source_types() -> list[str]:
    """Return a list of all registered source connector types."""
    return sorted(_source_registry.keys())


def list_destination_types() -> list[str]:
    """Return a list of all registered destination connector types."""
    return sorted(_destination_registry.keys())


def clear_registries() -> None:
    """Clear all registered connectors. Intended for testing only."""
    _source_registry.clear()
    _destination_registry.clear()

