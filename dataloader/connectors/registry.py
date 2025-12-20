"""Connector registry for managing connector factories.

This module provides a registry pattern for connector implementations,
allowing dynamic registration and retrieval of connector factories.
Connectors can implement read and/or write operations.
"""

from typing import Callable, Union, overload

from dataloader.connectors.base import Connector, Destination, Source
from dataloader.connectors.csv.config import CSVConnectorConfig
from dataloader.connectors.duckdb.config import DuckDBConnectorConfig
from dataloader.connectors.postgres.config import PostgresConnectorConfig
from dataloader.connectors.s3.config import S3ConnectorConfig
from dataloader.core.exceptions import ConnectorError
from dataloader.models.connector_config import ConnectorConfig, ConnectorConfigType
from dataloader.models.destination_config import DestinationConfig
from dataloader.models.source_config import SourceConfig

# Type aliases for factory signatures
# ConnectorConfigUnion accepts the new specific configs or legacy configs for backward compatibility
ConnectorConfigUnion = Union[
    ConnectorConfigType,
    SourceConfig,
    DestinationConfig,
]
ConnectorFactory = Callable[[ConnectorConfigUnion, dict], Connector]

# Legacy type aliases for backward compatibility
SourceFactory = Callable[[SourceConfig, dict], Source]
DestinationFactory = Callable[[DestinationConfig, dict], Destination]

# Global registry
_connector_registry: dict[str, ConnectorFactory] = {}

# Legacy registries (deprecated, kept for backward compatibility)
_source_registry: dict[str, SourceFactory] = {}
_destination_registry: dict[str, DestinationFactory] = {}


@overload
def register_connector(connector_type: str) -> Callable[[ConnectorFactory], ConnectorFactory]: ...


@overload
def register_connector(connector_type: str, factory: ConnectorFactory) -> None: ...


def register_connector(
    connector_type: str,
    factory: ConnectorFactory | None = None,
) -> Callable[[ConnectorFactory], ConnectorFactory] | None:
    """Register a connector factory.

    Can be used as a decorator or called directly:

        # As decorator
        @register_connector("postgres")
        def create_postgres_connector(config, connection):
            return PostgresConnector(config, connection)

        # Direct call
        register_connector("postgres", create_postgres_connector)

    Args:
        connector_type: Unique identifier for the connector (e.g., 'postgres', 's3', 'csv').
        factory: Factory function (optional if used as decorator).

    Raises:
        ConnectorError: If a connector with the same type is already registered.
    """

    def _register(f: ConnectorFactory) -> ConnectorFactory:
        if connector_type in _connector_registry:
            raise ConnectorError(
                f"Connector '{connector_type}' is already registered",
                context={"connector_type": connector_type},
            )
        _connector_registry[connector_type] = f
        return f

    if factory is not None:
        _register(factory)
        return None

    return _register


@overload
def register_source(connector_type: str) -> Callable[[SourceFactory], SourceFactory]: ...


@overload
def register_source(connector_type: str, factory: SourceFactory) -> None: ...


def register_source(
    connector_type: str,
    factory: SourceFactory | None = None,
) -> Callable[[SourceFactory], SourceFactory] | None:
    """Register a source connector factory.

    .. deprecated::
        Use :func:`register_connector` instead. This function is kept for backward compatibility.

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
        # Also register in unified registry for backward compatibility
        if connector_type not in _connector_registry:
            _connector_registry[connector_type] = f  # type: ignore
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

    .. deprecated::
        Use :func:`register_connector` instead. This function is kept for backward compatibility.

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
        # Also register in unified registry for backward compatibility
        if connector_type not in _connector_registry:
            _connector_registry[connector_type] = f  # type: ignore
        return f

    if factory is not None:
        _register(factory)
        return None

    return _register


def get_connector(
    connector_type: str,
    config: ConnectorConfigUnion,
    connection: dict,
) -> Connector:
    """Create a connector instance using the registered factory.

    Args:
        connector_type: The connector type to instantiate.
        config: Connector configuration from the recipe. Can be:
            - Connector-specific config (PostgresConnectorConfig, S3ConnectorConfig, etc.)
            - Legacy SourceConfig or DestinationConfig (for backward compatibility)
        connection: Connection parameters (host, port, credentials, etc.).

    Returns:
        A Connector instance configured for reading and/or writing.

    Raises:
        ConnectorError: If the connector type is not registered.
    """
    factory = _connector_registry.get(connector_type)
    if factory is None:
        available = ", ".join(sorted(_connector_registry.keys())) or "(none)"
        raise ConnectorError(
            f"Unknown connector type: '{connector_type}'",
            context={"connector_type": connector_type, "available_types": available},
        )
    return factory(config, connection)


def get_source(
    connector_type: str,
    config: SourceConfig,
    connection: dict,
) -> Source:
    """Create a source connector instance using the registered factory.

    .. deprecated::
        Use :func:`get_connector` instead. This function is kept for backward compatibility.

    Args:
        connector_type: The connector type to instantiate.
        config: Source configuration from the recipe.
        connection: Connection parameters (host, port, credentials, etc.).

    Returns:
        A Source instance configured for reading.

    Raises:
        ConnectorError: If the connector type is not registered.
    """
    # Try unified registry first, fall back to legacy registry
    factory = _connector_registry.get(connector_type) or _source_registry.get(connector_type)
    if factory is None:
        available = ", ".join(sorted(set(_connector_registry.keys()) | set(_source_registry.keys()))) or "(none)"
        raise ConnectorError(
            f"Unknown source connector type: '{connector_type}'",
            context={"connector_type": connector_type, "available_types": available},
        )
    return factory(config, connection)  # type: ignore


def get_destination(
    connector_type: str,
    config: DestinationConfig,
    connection: dict,
) -> Destination:
    """Create a destination connector instance using the registered factory.

    .. deprecated::
        Use :func:`get_connector` instead. This function is kept for backward compatibility.

    Args:
        connector_type: The connector type to instantiate.
        config: Destination configuration from the recipe.
        connection: Connection parameters (host, port, credentials, etc.).

    Returns:
        A Destination instance configured for writing.

    Raises:
        ConnectorError: If the connector type is not registered.
    """
    # Try unified registry first, fall back to legacy registry
    factory = _connector_registry.get(connector_type) or _destination_registry.get(connector_type)
    if factory is None:
        available = ", ".join(sorted(set(_connector_registry.keys()) | set(_destination_registry.keys()))) or "(none)"
        raise ConnectorError(
            f"Unknown destination connector type: '{connector_type}'",
            context={"connector_type": connector_type, "available_types": available},
        )
    return factory(config, connection)  # type: ignore


def list_connector_types() -> list[str]:
    """Return a list of all registered connector types."""
    return sorted(_connector_registry.keys())


def list_source_types() -> list[str]:
    """Return a list of all registered source connector types.

    .. deprecated::
        Use :func:`list_connector_types` instead. This function is kept for backward compatibility.
    """
    return sorted(set(_connector_registry.keys()) | set(_source_registry.keys()))


def list_destination_types() -> list[str]:
    """Return a list of all registered destination connector types.

    .. deprecated::
        Use :func:`list_connector_types` instead. This function is kept for backward compatibility.
    """
    return sorted(set(_connector_registry.keys()) | set(_destination_registry.keys()))


def clear_registries() -> None:
    """Clear all registered connectors. Intended for testing only."""
    _connector_registry.clear()
    _source_registry.clear()
    _destination_registry.clear()

