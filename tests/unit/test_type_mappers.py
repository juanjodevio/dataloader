"""Tests for connector type mappers."""

import pytest

import pyarrow as pa

from dataloader.connectors.duckdb.type_mapper import DuckDBTypeMapper
from dataloader.connectors.postgres.type_mapper import PostgresTypeMapper


class TestPostgresTypeMapper:
    """Tests for PostgresTypeMapper."""

    def test_arrow_to_postgres_string(self):
        """Test mapping Arrow string types to PostgreSQL."""
        mapper = PostgresTypeMapper()
        assert mapper.arrow_to_connector_type(pa.string()) == "VARCHAR"
        assert mapper.arrow_to_connector_type(pa.large_string()) == "VARCHAR"

    def test_arrow_to_postgres_integer(self):
        """Test mapping Arrow integer types to PostgreSQL."""
        mapper = PostgresTypeMapper()
        assert mapper.arrow_to_connector_type(pa.int64()) == "BIGINT"
        assert mapper.arrow_to_connector_type(pa.int32()) == "INTEGER"
        assert mapper.arrow_to_connector_type(pa.int16()) == "SMALLINT"

    def test_arrow_to_postgres_float(self):
        """Test mapping Arrow float types to PostgreSQL."""
        mapper = PostgresTypeMapper()
        assert mapper.arrow_to_connector_type(pa.float64()) == "DOUBLE PRECISION"
        assert mapper.arrow_to_connector_type(pa.float32()) == "REAL"

    def test_arrow_to_postgres_other_types(self):
        """Test mapping other Arrow types to PostgreSQL."""
        mapper = PostgresTypeMapper()
        assert mapper.arrow_to_connector_type(pa.bool_()) == "BOOLEAN"
        assert mapper.arrow_to_connector_type(pa.timestamp("us")) == "TIMESTAMP"
        assert mapper.arrow_to_connector_type(pa.date32()) == "DATE"

    def test_postgres_to_arrow_bidirectional(self):
        """Test bidirectional mapping PostgreSQL to Arrow."""
        mapper = PostgresTypeMapper()

        # Test common PostgreSQL types
        assert mapper.connector_type_to_arrow("VARCHAR").equals(pa.string())
        assert mapper.connector_type_to_arrow("BIGINT").equals(pa.int64())
        assert mapper.connector_type_to_arrow("INTEGER").equals(pa.int32())
        assert mapper.connector_type_to_arrow("DOUBLE PRECISION").equals(pa.float64())
        assert mapper.connector_type_to_arrow("BOOLEAN").equals(pa.bool_())
        assert mapper.connector_type_to_arrow("TIMESTAMP").equals(pa.timestamp("us"))
        assert mapper.connector_type_to_arrow("DATE").equals(pa.date32())


class TestDuckDBTypeMapper:
    """Tests for DuckDBTypeMapper."""

    def test_arrow_to_duckdb_string(self):
        """Test mapping Arrow string types to DuckDB."""
        mapper = DuckDBTypeMapper()
        assert mapper.arrow_to_connector_type(pa.string()) == "VARCHAR"
        assert mapper.arrow_to_connector_type(pa.large_string()) == "VARCHAR"

    def test_arrow_to_duckdb_integer(self):
        """Test mapping Arrow integer types to DuckDB."""
        mapper = DuckDBTypeMapper()
        assert mapper.arrow_to_connector_type(pa.int64()) == "BIGINT"
        assert mapper.arrow_to_connector_type(pa.int32()) == "INTEGER"

    def test_arrow_to_duckdb_float(self):
        """Test mapping Arrow float types to DuckDB."""
        mapper = DuckDBTypeMapper()
        assert mapper.arrow_to_connector_type(pa.float64()) == "DOUBLE"
        assert mapper.arrow_to_connector_type(pa.float32()) == "FLOAT"

    def test_arrow_to_duckdb_other_types(self):
        """Test mapping other Arrow types to DuckDB."""
        mapper = DuckDBTypeMapper()
        assert mapper.arrow_to_connector_type(pa.bool_()) == "BOOLEAN"
        assert mapper.arrow_to_connector_type(pa.timestamp("us")) == "TIMESTAMP"
        assert mapper.arrow_to_connector_type(pa.date32()) == "DATE"

    def test_duckdb_to_arrow_bidirectional(self):
        """Test bidirectional mapping DuckDB to Arrow."""
        mapper = DuckDBTypeMapper()

        # Test common DuckDB types
        assert mapper.connector_type_to_arrow("VARCHAR").equals(pa.string())
        assert mapper.connector_type_to_arrow("BIGINT").equals(pa.int64())
        assert mapper.connector_type_to_arrow("INTEGER").equals(pa.int32())
        assert mapper.connector_type_to_arrow("DOUBLE").equals(pa.float64())
        assert mapper.connector_type_to_arrow("BOOLEAN").equals(pa.bool_())
        assert mapper.connector_type_to_arrow("TIMESTAMP").equals(pa.timestamp("us"))
        assert mapper.connector_type_to_arrow("DATE").equals(pa.date32())
