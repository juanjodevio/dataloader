"""Connector registry for managing connector factories.

This module provides a registry pattern for connector implementations,
allowing dynamic registration and retrieval of connector factories.
Connectors can implement read and/or write operations.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Union, overload

from dataloader.connectors.base import Connector
from dataloader.core.exceptions import ConnectorError
from dataloader.models.destination_config import DestinationConfig
from dataloader.models.source_config import SourceConfig

if TYPE_CHECKING:
    from dataloader.models.connector_config import ConnectorConfigType

# Type aliases for factory signatures
# Using __future__.annotations makes all annotations strings, avoiding circular imports
# We use string literal for ConnectorConfigType to avoid importing it at runtime
ConnectorConfigUnion = Union[
    "ConnectorConfigType",  # Forward reference - only resolved at type-checking time
    SourceConfig,
    DestinationConfig,
]
ConnectorFactory = Callable[[ConnectorConfigUnion], Connector]

# Global registry
_connector_registry: dict[str, ConnectorFactory] = {}


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
        def create_postgres_connector(config):
            return PostgresConnector(config)

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


def get_connector(
    connector_type: str,
    config: ConnectorConfigUnion,
) -> Connector:
    """Create a connector instance using the registered factory.

    Args:
        connector_type: The connector type to instantiate.
        config: Connector configuration from the recipe. Can be:
            - Connector-specific config (PostgresConnectorConfig, S3ConnectorConfig, etc.)
            - SourceConfig or DestinationConfig
            All connection parameters should be in the config object.

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
    return factory(config)


def list_connector_types() -> list[str]:
    """Return a list of all registered connector types."""
    return sorted(_connector_registry.keys())


def clear_registries() -> None:
    """Clear all registered connectors. Intended for testing only."""
    _connector_registry.clear()

