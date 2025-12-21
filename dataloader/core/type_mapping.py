"""Shared type mapping utilities for Arrow types.

This module provides centralized type mapping functions used by transforms
and connectors to ensure consistency across the codebase.
"""

import pyarrow as pa

# String type names to Arrow types (used by cast transform)
STRING_TO_ARROW_TYPE: dict[str, pa.DataType] = {
    "str": pa.string(),
    "string": pa.string(),
    "int": pa.int64(),
    "integer": pa.int64(),
    "float": pa.float64(),
    "double": pa.float64(),
    "bool": pa.bool_(),
    "boolean": pa.bool_(),
    "datetime": pa.timestamp("us"),  # Microsecond precision timestamp
    "timestamp": pa.timestamp("us"),
    "date": pa.date32(),
}


def string_to_arrow_type(type_name: str) -> pa.DataType:
    """Convert string type name to Arrow type.

    Args:
        type_name: String type name (e.g., "int", "str", "datetime")

    Returns:
        Arrow DataType

    Raises:
        ValueError: If type_name is not supported
    """
    type_name_lower = type_name.lower()
    arrow_type = STRING_TO_ARROW_TYPE.get(type_name_lower)
    if arrow_type is None:
        raise ValueError(
            f"Unsupported type name: {type_name}. "
            f"Supported types: {list(STRING_TO_ARROW_TYPE.keys())}"
        )
    return arrow_type


def arrow_type_to_string(arrow_type: pa.DataType) -> str:
    """Convert Arrow type to canonical string name.

    Args:
        arrow_type: Arrow DataType

    Returns:
        Canonical string type name
    """
    # Reverse lookup - find first matching type name
    for name, mapped_type in STRING_TO_ARROW_TYPE.items():
        if arrow_type.equals(mapped_type):
            # Return the shortest canonical name
            if name in ("str", "int", "float", "bool", "datetime"):
                return name
    return str(arrow_type)
