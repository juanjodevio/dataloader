"""Unit tests for PostgresConnector."""

from datetime import datetime
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from dataloader.connectors.postgres.connector import (
    PostgresConnector,
    create_postgres_connector,
)
from dataloader.core.batch import ArrowBatch
from dataloader.core.exceptions import ConnectorError
from dataloader.core.state import State
from dataloader.models.destination_config import DestinationConfig
from dataloader.models.source_config import IncrementalConfig, SourceConfig


class TestPostgresConnector:
    """Tests for PostgresConnector with SQLAlchemy."""

    @pytest.fixture
    def postgres_config(self) -> SourceConfig:
        """Create a sample Postgres source config."""
        return SourceConfig(
            type="postgres",
            host="localhost",
            port=5432,
            database="testdb",
            user="testuser",
            password="testpass",
            table="users",
            db_schema="public",
        )

    @pytest.fixture
    def incremental_postgres_config(self) -> SourceConfig:
        """Create a Postgres config with incremental settings."""
        return SourceConfig(
            type="postgres",
            host="localhost",
            port=5432,
            database="testdb",
            user="testuser",
            password="testpass",
            table="users",
            db_schema="public",
            incremental=IncrementalConfig(
                strategy="cursor",
                cursor_column="updated_at",
            ),
        )

    def test_postgres_connector_initialization(self, postgres_config: SourceConfig):
        """Test that PostgresConnector initializes correctly."""
        connector = PostgresConnector(postgres_config)

        assert connector._batch_size == PostgresConnector.DEFAULT_BATCH_SIZE
        assert connector._engine is None

    def test_postgres_connection_url_with_password(self, postgres_config: SourceConfig):
        """Test connection URL building with password."""
        connector = PostgresConnector(postgres_config)
        url = connector._build_connection_url()

        assert "postgresql+psycopg2://" in url
        assert "testuser:testpass@" in url
        assert "localhost:5432/testdb" in url

    def test_postgres_connection_url_without_password(
        self, postgres_config: SourceConfig
    ):
        """Test connection URL building without password."""
        postgres_config.password = None
        connector = PostgresConnector(postgres_config)
        url = connector._build_connection_url()

        assert "testuser@localhost" in url
        assert ":testpass" not in url

    def test_postgres_connection_url_custom_dialect(
        self, postgres_config: SourceConfig
    ):
        """Test connection URL with custom dialect (via config)."""
        # Note: Dialect is now fixed to postgresql+psycopg2 in PostgresConnector
        # This test verifies the default dialect
        connector = PostgresConnector(postgres_config)
        url = connector._build_connection_url()

        assert "postgresql+psycopg2://" in url

    def test_postgres_connector_default_port(self, postgres_config: SourceConfig):
        """Test that default port is used when not specified."""
        postgres_config.port = None
        connector = PostgresConnector(postgres_config)
        url = connector._build_connection_url()

        assert ":5432/" in url

    @patch("dataloader.connectors.postgres.connector.create_engine")
    def test_postgres_engine_creation_error(
        self, mock_create_engine: MagicMock, postgres_config: SourceConfig
    ):
        """Test that engine creation errors raise ConnectorError."""
        from sqlalchemy.exc import SQLAlchemyError

        mock_create_engine.side_effect = SQLAlchemyError("Connection refused")
        connector = PostgresConnector(postgres_config)

        with pytest.raises(ConnectorError) as exc_info:
            connector._get_engine()

        assert "Failed to create database engine" in str(exc_info.value)
        assert exc_info.value.context["host"] == "localhost"

    @patch("dataloader.connectors.postgres.connector.pd.read_sql")
    @patch("dataloader.connectors.postgres.connector.create_engine")
    @patch("dataloader.connectors.postgres.connector.inspect")
    def test_postgres_read_batches(
        self,
        mock_inspect: MagicMock,
        mock_create_engine: MagicMock,
        mock_read_sql: MagicMock,
        postgres_config: SourceConfig,
    ):
        """Test reading batches from Postgres using SQLAlchemy."""
        import pandas as pd

        # Setup mock engine
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        # Setup mock inspector for schema
        mock_inspector = MagicMock()
        mock_inspect.return_value = mock_inspector
        mock_inspector.get_columns.return_value = [
            {"name": "id", "type": "INTEGER"},
            {"name": "name", "type": "VARCHAR(100)"},
        ]

        # Setup mock pandas DataFrame from read_sql
        mock_df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
        # Mock read_sql to return an iterator (chunksize behavior)
        mock_read_sql.return_value = iter([mock_df])

        connector = PostgresConnector(postgres_config)
        state = State()

        batches = list(connector.read_batches(state))

        assert len(batches) == 1
        assert batches[0].columns == ["id", "name"]
        assert batches[0].rows == [[1, "Alice"], [2, "Bob"]]
        assert batches[0].metadata["source_type"] == "postgres"
        assert batches[0].metadata["table"] == "users"
        mock_engine.dispose.assert_called_once()

    @patch("dataloader.connectors.postgres.connector.create_engine")
    @patch("dataloader.connectors.postgres.connector.inspect")
    def test_postgres_incremental_query(
        self,
        mock_inspect: MagicMock,
        mock_create_engine: MagicMock,
        incremental_postgres_config: SourceConfig,
    ):
        """Test that incremental loads use cursor filtering."""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        mock_inspector = MagicMock()
        mock_inspect.return_value = mock_inspector
        mock_inspector.get_columns.return_value = [
            {"name": "id", "type": "INTEGER"},
            {"name": "updated_at", "type": "TIMESTAMP"},
        ]

        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        mock_result = MagicMock()
        mock_conn.execution_options.return_value.execute.return_value = mock_result
        mock_result.fetchmany.side_effect = [[], []]

        connector = PostgresConnector(incremental_postgres_config)
        state = State(cursor_values={"updated_at": "2024-01-01"})

        # Build query to test
        query, params = connector._build_query(state)

        assert 'WHERE "updated_at" > :cursor_value' in query
        assert 'ORDER BY "updated_at"' in query
        assert params["cursor_value"] == "2024-01-01"

    def test_create_postgres_connector_factory(self, postgres_config: SourceConfig):
        """Test the factory function creates PostgresConnector."""
        connector = create_postgres_connector(postgres_config)
        assert isinstance(connector, PostgresConnector)


class TestPostgresConnectorRegistration:
    """Tests for PostgresConnector registration."""

    @pytest.fixture(autouse=True)
    def ensure_registration(self):
        """Ensure built-in connectors are registered before each test."""
        from dataloader.connectors import reregister_builtins

        reregister_builtins()

    def test_connectors_registered(self):
        """Test that built-in connectors are registered."""
        from dataloader.connectors import list_connector_types

        types = list_connector_types()
        assert "postgres" in types
        assert "duckdb" in types
        assert "filestore" in types

    def test_get_connector_postgres(self):
        """Test getting PostgresConnector via registry."""
        from dataloader.connectors import get_connector

        config = SourceConfig(
            type="postgres",
            host="localhost",
            database="test",
            user="user",
            table="users",
        )
        connector = get_connector("postgres", config)

        assert isinstance(connector, PostgresConnector)


class TestPostgresConnectorFullRefresh:
    """Tests for full_refresh functionality in PostgresConnector."""

    @pytest.fixture
    def destination_config(self) -> DestinationConfig:
        """Create a Postgres destination config."""
        return DestinationConfig(
            type="postgres",
            host="localhost",
            port=5432,
            database="testdb",
            user="testuser",
            password="testpass",
            table="users",
            db_schema="public",
            write_mode="overwrite",
        )

    @pytest.fixture
    def sample_batch(self) -> ArrowBatch:
        """Create a sample batch."""
        return ArrowBatch.from_rows(
            columns=["id", "name"],
            rows=[[1, "Alice"], [2, "Bob"]],
            metadata={},
        )

    @patch("dataloader.connectors.postgres.connector.PostgresConnector._insert_batch")
    @patch("dataloader.connectors.postgres.connector.create_engine")
    @patch("dataloader.connectors.postgres.connector.inspect")
    def test_full_refresh_overwrite_drops_table(
        self,
        mock_inspect: MagicMock,
        mock_create_engine: MagicMock,
        mock_insert_batch: MagicMock,
        destination_config: DestinationConfig,
        sample_batch: ArrowBatch,
    ):
        """Test that full_refresh=True with overwrite mode drops table."""
        from sqlalchemy import text
        from sqlalchemy.engine import Engine

        mock_engine = MagicMock(spec=Engine)
        mock_create_engine.return_value = mock_engine
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        mock_inspector = MagicMock()
        mock_inspect.return_value = mock_inspector
        mock_inspector.get_columns.return_value = []

        connector = PostgresConnector(destination_config)
        state = State(metadata={"full_refresh": True})

        connector.write_batch(sample_batch, state)

        # Verify DROP TABLE was called (not TRUNCATE)
        # Check the SQL strings in the execute calls
        drop_calls = [
            execute_call
            for execute_call in mock_conn.execute.call_args_list
            if len(execute_call[0]) > 0 and "DROP TABLE" in str(execute_call[0][0])
        ]
        truncate_calls = [
            execute_call
            for execute_call in mock_conn.execute.call_args_list
            if len(execute_call[0]) > 0 and "TRUNCATE" in str(execute_call[0][0])
        ]

        assert len(drop_calls) > 0, "DROP TABLE should be called with full_refresh=True"
        assert (
            len(truncate_calls) == 0
        ), "TRUNCATE should not be called with full_refresh=True"
        mock_insert_batch.assert_called_once()

    @patch("dataloader.connectors.postgres.connector.PostgresConnector._insert_batch")
    @patch("dataloader.connectors.postgres.connector.create_engine")
    @patch("dataloader.connectors.postgres.connector.inspect")
    def test_default_overwrite_truncates_table(
        self,
        mock_inspect: MagicMock,
        mock_create_engine: MagicMock,
        mock_insert_batch: MagicMock,
        destination_config: DestinationConfig,
        sample_batch: ArrowBatch,
    ):
        """Test that full_refresh=False with overwrite mode truncates table (not drops)."""
        from sqlalchemy import text
        from sqlalchemy.engine import Engine

        mock_engine = MagicMock(spec=Engine)
        mock_create_engine.return_value = mock_engine
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        mock_inspector = MagicMock()
        mock_inspect.return_value = mock_inspector
        # Table exists, so truncate should work
        mock_inspector.get_columns.return_value = [{"name": "id"}, {"name": "name"}]

        connector = PostgresConnector(destination_config)
        state = State(metadata={"full_refresh": False})

        connector.write_batch(sample_batch, state)

        # Verify TRUNCATE was called (not DROP)
        truncate_calls = [
            execute_call
            for execute_call in mock_conn.execute.call_args_list
            if len(execute_call[0]) > 0 and "TRUNCATE" in str(execute_call[0][0])
        ]
        drop_calls = [
            execute_call
            for execute_call in mock_conn.execute.call_args_list
            if len(execute_call[0]) > 0 and "DROP TABLE" in str(execute_call[0][0])
        ]

        assert (
            len(truncate_calls) > 0
        ), "TRUNCATE should be called with full_refresh=False"
        assert (
            len(drop_calls) == 0
        ), "DROP TABLE should not be called with full_refresh=False"
        mock_insert_batch.assert_called_once()

    @patch("dataloader.connectors.postgres.connector.PostgresConnector._insert_batch")
    @patch("dataloader.connectors.postgres.connector.create_engine")
    @patch("dataloader.connectors.postgres.connector.inspect")
    def test_full_refresh_append_drops_table(
        self,
        mock_inspect: MagicMock,
        mock_create_engine: MagicMock,
        mock_insert_batch: MagicMock,
        destination_config: DestinationConfig,
        sample_batch: ArrowBatch,
    ):
        """Test that full_refresh=True with append mode drops and recreates table."""
        from sqlalchemy import text
        from sqlalchemy.engine import Engine

        destination_config.write_mode = "append"

        mock_engine = MagicMock(spec=Engine)
        mock_create_engine.return_value = mock_engine
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        mock_inspector = MagicMock()
        mock_inspect.return_value = mock_inspector
        mock_inspector.get_columns.return_value = []

        connector = PostgresConnector(destination_config)
        state = State(metadata={"full_refresh": True})

        connector.write_batch(sample_batch, state)

        # Verify DROP TABLE was called
        drop_calls = [
            execute_call
            for execute_call in mock_conn.execute.call_args_list
            if len(execute_call[0]) > 0 and "DROP TABLE" in str(execute_call[0][0])
        ]

        assert (
            len(drop_calls) > 0
        ), "DROP TABLE should be called with full_refresh=True and append mode"
        mock_insert_batch.assert_called_once()

    @patch("dataloader.connectors.postgres.connector.PostgresConnector._insert_batch")
    @patch("dataloader.connectors.postgres.connector.create_engine")
    @patch("dataloader.connectors.postgres.connector.inspect")
    def test_full_refresh_only_drops_once_with_multiple_batches(
        self,
        mock_inspect: MagicMock,
        mock_create_engine: MagicMock,
        mock_insert_batch: MagicMock,
        destination_config: DestinationConfig,
        sample_batch: ArrowBatch,
    ):
        """Test that full_refresh only drops table on first batch, not subsequent batches."""
        from sqlalchemy import text
        from sqlalchemy.engine import Engine

        mock_engine = MagicMock(spec=Engine)
        mock_create_engine.return_value = mock_engine
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        mock_inspector = MagicMock()
        mock_inspect.return_value = mock_inspector
        mock_inspector.get_columns.return_value = []

        connector = PostgresConnector(destination_config)
        state = State(metadata={"full_refresh": True})

        batch2 = ArrowBatch.from_rows(
            columns=["id", "name"],
            rows=[[3, "Charlie"]],
            metadata={},
        )

        # Write first batch
        connector.write_batch(sample_batch, state)
        # Write second batch
        connector.write_batch(batch2, state)

        # Verify DROP TABLE was called exactly once (only on first batch)
        drop_calls = [
            execute_call
            for execute_call in mock_conn.execute.call_args_list
            if len(execute_call[0]) > 0 and "DROP TABLE" in str(execute_call[0][0])
        ]

        assert (
            len(drop_calls) == 1
        ), "DROP TABLE should only be called once, not on every batch"
        assert (
            mock_insert_batch.call_count == 2
        ), "Insert should be called for each batch"
