"""Connector protocols and registry for data sources and destinations.

This module exposes:
- Source, Destination: Protocols defining connector interfaces
- Registry functions: register_source, register_destination, get_source, get_destination
- Utility functions: list_source_types, list_destination_types, clear_registries
"""

from dataloader.connectors.base import Destination, Source
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
    # Retrieval functions
    "get_source",
    "get_destination",
    # Utility functions
    "list_source_types",
    "list_destination_types",
    "clear_registries",
]

