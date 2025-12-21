"""Type mapper for PostgreSQL connector."""

import pyarrow as pa

from dataloader.core.type_mapping import TypeMapper


class PostgresTypeMapper:
    """Type mapper for PostgreSQL connector.

    Maps between Arrow types and PostgreSQL types for schema creation
    and data type conversion.
    """

    def arrow_to_connector_type(self, arrow_type: pa.DataType) -> str:
        """Map Arrow type to PostgreSQL type.

        Args:
            arrow_type: PyArrow DataType

        Returns:
            PostgreSQL type string (e.g., "VARCHAR", "BIGINT", "TIMESTAMP")
        """
        if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
            return "VARCHAR"
        elif pa.types.is_integer(arrow_type):
            if pa.types.is_int64(arrow_type):
                return "BIGINT"
            elif pa.types.is_int32(arrow_type):
                return "INTEGER"
            elif pa.types.is_int16(arrow_type):
                return "SMALLINT"
            else:
                return "INTEGER"
        elif pa.types.is_floating(arrow_type):
            if pa.types.is_float64(arrow_type):
                return "DOUBLE PRECISION"
            else:
                return "REAL"
        elif pa.types.is_boolean(arrow_type):
            return "BOOLEAN"
        elif pa.types.is_timestamp(arrow_type):
            return "TIMESTAMP"
        elif pa.types.is_date(arrow_type) or pa.types.is_date32(arrow_type):
            return "DATE"
        else:
            # Default to VARCHAR for unknown types
            return "VARCHAR"

    def connector_type_to_arrow(self, connector_type: str) -> pa.DataType:
        """Map PostgreSQL type to Arrow type.

        Args:
            connector_type: PostgreSQL type string (e.g., "VARCHAR", "BIGINT")

        Returns:
            PyArrow DataType
        """
        pg_type_upper = connector_type.upper()

        # Handle common PostgreSQL types
        if pg_type_upper in ("VARCHAR", "TEXT", "CHAR", "CHARACTER"):
            return pa.string()
        elif pg_type_upper in ("BIGINT", "INT8"):
            return pa.int64()
        elif pg_type_upper in ("INTEGER", "INT", "INT4"):
            return pa.int32()
        elif pg_type_upper in ("SMALLINT", "INT2"):
            return pa.int16()
        elif pg_type_upper in ("DOUBLE PRECISION", "FLOAT8"):
            return pa.float64()
        elif pg_type_upper in ("REAL", "FLOAT4"):
            return pa.float32()
        elif pg_type_upper == "BOOLEAN":
            return pa.bool_()
        elif pg_type_upper in ("TIMESTAMP", "TIMESTAMPTZ"):
            return pa.timestamp("us")
        elif pg_type_upper == "DATE":
            return pa.date32()
        else:
            # Default to string for unknown types
            return pa.string()
