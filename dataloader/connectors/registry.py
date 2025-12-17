"""Connector registry for managing source and destination factories.

This module provides a registry pattern for connector implementations,
allowing dynamic registration and retrieval of source/destination factories.
"""

from typing import Callable, overload

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


@overload
def register_source(connector_type: str) -> Callable[[SourceFactory], SourceFactory]: ...


@overload
def register_source(connector_type: str, factory: SourceFactory) -> None: ...


def register_source(
    connector_type: str,
    factory: SourceFactory | None = None,
) -> Callable[[SourceFactory], SourceFactory] | None:
    """Register a source connector factory.

    Can be used as a decorator or called directly:

        # As decorator
        @register_source("postgres")
        def create_postgres_source(config, connection):
            return PostgresSource(config, connection)

        # Direct call
        register_source("postgres", create_postgres_source)

    Args:
        connector_type: Unique identifier for the connector (e.g., 'postgres', 'csv').
        factory: Factory function (optional if used as decorator).

    Raises:
        ConnectorError: If a connector with the same type is already registered.
    """

    def _register(f: SourceFactory) -> SourceFactory:
        if connector_type in _source_registry:
            raise ConnectorError(
                f"Source connector '{connector_type}' is already registered",
                context={"connector_type": connector_type},
            )
        _source_registry[connector_type] = f
        return f

    if factory is not None:
        _register(factory)
        return None

    return _register


@overload
def register_destination(connector_type: str) -> Callable[[DestinationFactory], DestinationFactory]: ...


@overload
def register_destination(connector_type: str, factory: DestinationFactory) -> None: ...


def register_destination(
    connector_type: str,
    factory: DestinationFactory | None = None,
) -> Callable[[DestinationFactory], DestinationFactory] | None:
    """Register a destination connector factory.

    Can be used as a decorator or called directly:

        # As decorator
        @register_destination("duckdb")
        def create_duckdb_destination(config, connection):
            return DuckDBDestination(config, connection)

        # Direct call
        register_destination("duckdb", create_duckdb_destination)

    Args:
        connector_type: Unique identifier for the connector (e.g., 'duckdb', 's3').
        factory: Factory function (optional if used as decorator).

    Raises:
        ConnectorError: If a connector with the same type is already registered.
    """

    def _register(f: DestinationFactory) -> DestinationFactory:
        if connector_type in _destination_registry:
            raise ConnectorError(
                f"Destination connector '{connector_type}' is already registered",
                context={"connector_type": connector_type},
            )
        _destination_registry[connector_type] = f
        return f

    if factory is not None:
        _register(factory)
        return None

    return _register


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

