"""Tests for connector protocols and registry."""

from typing import Iterable

import pytest

from dataloader.connectors import (
    Connector,
    clear_registries,
    get_connector,
    list_connector_types,
    register_connector,
)
from dataloader.core.batch import ArrowBatch, Batch
from dataloader.core.exceptions import ConnectorError
from dataloader.core.state import State
from dataloader.models.connector_config import ConnectorConfig
from dataloader.models.destination_config import DestinationConfig
from dataloader.models.source_config import SourceConfig


class MockConnector:
    """Mock connector implementation for testing."""

    def __init__(self, config: ConnectorConfig):
        self.config = config
        self.written_batches: list[Batch] = []

    def read_batches(self, state: State) -> Iterable[Batch]:
        """Read batches - supports read operations."""
        yield ArrowBatch.from_rows(
            columns=["id", "name"],
            rows=[[1, "test"]],
            metadata={"source": "mock"},
        )

    def write_batch(self, batch: Batch, state: State) -> None:
        """Write batch - supports write operations."""
        self.written_batches.append(batch)


class MockReadOnlyConnector:
    """Mock read-only connector implementation for testing."""

    def __init__(self, config: ConnectorConfig):
        self.config = config

    def read_batches(self, state: State) -> Iterable[Batch]:
        """Read batches - read-only connector."""
        yield ArrowBatch.from_rows(
            columns=["id", "name"],
            rows=[[1, "test"]],
            metadata={"source": "mock"},
        )


class MockWriteOnlyConnector:
    """Mock write-only connector implementation for testing."""

    def __init__(self, config: ConnectorConfig):
        self.config = config
        self.written_batches: list[Batch] = []

    def write_batch(self, batch: Batch, state: State) -> None:
        """Write batch - write-only connector."""
        self.written_batches.append(batch)


def create_mock_connector(config: ConnectorConfig) -> MockConnector:
    """Factory function for MockConnector."""
    return MockConnector(config)


@pytest.fixture(autouse=True)
def clean_registry():
    """Clear registries before and after each test."""
    clear_registries()
    yield
    clear_registries()


class TestConnectorProtocol:
    """Tests for Connector protocol compliance."""

    def test_mock_connector_satisfies_protocol(self):
        """Verify MockConnector implements the Connector protocol."""
        config = SourceConfig(
            type="postgres",
            host="localhost",
            database="test",
            user="user",
            table="users",
        )
        connector = MockConnector(config)

        assert isinstance(connector, Connector)

    def test_mock_connector_read_batches(self):
        """Test that read_batches yields valid batches."""
        config = SourceConfig(
            type="postgres",
            host="localhost",
            database="test",
            user="user",
            table="users",
        )
        connector = MockConnector(config)
        state = State()

        batches = list(connector.read_batches(state))

        assert len(batches) == 1
        assert batches[0].columns == ["id", "name"]
        assert batches[0].rows == [[1, "test"]]

    def test_mock_connector_write_batch(self):
        """Test that write_batch accepts valid batches."""
        config = DestinationConfig(
            type="duckdb",
            host="localhost",
            database="test.db",
            user="user",
            table="output",
        )
        connector = MockConnector(config)
        state = State()
        batch = ArrowBatch.from_rows(columns=["id"], rows=[[1]])

        connector.write_batch(batch, state)

        assert len(connector.written_batches) == 1
        assert connector.written_batches[0] is batch

    def test_read_only_connector(self):
        """Test that read-only connector works correctly."""
        config = SourceConfig(
            type="postgres",
            host="localhost",
            database="test",
            user="user",
            table="users",
        )
        connector = MockReadOnlyConnector(config)
        state = State()

        batches = list(connector.read_batches(state))

        assert len(batches) == 1
        # Read-only connector should not have write_batch
        assert not hasattr(connector, "write_batch") or not callable(
            getattr(connector, "write_batch", None)
        )

    def test_write_only_connector(self):
        """Test that write-only connector works correctly."""
        config = DestinationConfig(
            type="duckdb",
            host="localhost",
            database="test.db",
            user="user",
            table="output",
        )
        connector = MockWriteOnlyConnector(config)
        state = State()
        batch = ArrowBatch.from_rows(columns=["id"], rows=[[1]])

        connector.write_batch(batch, state)

        assert len(connector.written_batches) == 1
        # Write-only connector should not have read_batches
        assert not hasattr(connector, "read_batches") or not callable(
            getattr(connector, "read_batches", None)
        )


class TestConnectorRegistry:
    """Tests for unified connector registry functions."""

    def test_register_and_get_connector(self):
        """Test registering and retrieving a connector factory."""
        config = SourceConfig(
            type="postgres",
            host="localhost",
            database="test",
            user="user",
            table="users",
        )

        register_connector("mock", create_mock_connector)

        connector = get_connector("mock", config)

        assert isinstance(connector, MockConnector)
        assert connector.config is config

    def test_register_duplicate_connector_raises(self):
        """Test that registering duplicate connector type raises ConnectorError."""
        register_connector("mock", create_mock_connector)

        with pytest.raises(ConnectorError) as exc_info:
            register_connector("mock", create_mock_connector)

        assert "already registered" in str(exc_info.value)
        assert exc_info.value.context["connector_type"] == "mock"

    def test_get_unknown_connector_raises(self):
        """Test that getting unknown connector type raises ConnectorError."""
        config = SourceConfig(
            type="postgres",
            host="localhost",
            database="test",
            user="user",
            table="users",
        )

        with pytest.raises(ConnectorError) as exc_info:
            get_connector("unknown", config)

        assert "Unknown connector type" in str(exc_info.value)
        assert exc_info.value.context["connector_type"] == "unknown"

    def test_get_unknown_connector_shows_available(self):
        """Test that error message includes available connector types."""
        register_connector("postgres", create_mock_connector)
        register_connector("duckdb", create_mock_connector)
        config = SourceConfig(
            type="postgres",
            host="localhost",
            database="test",
            user="user",
            table="users",
        )

        with pytest.raises(ConnectorError) as exc_info:
            get_connector("unknown", config)

        # Check that both types are present (order may vary due to sorting)
        available = exc_info.value.context["available_types"]
        assert "postgres" in available
        assert "duckdb" in available

    def test_list_connector_types(self):
        """Test listing registered connector types."""
        register_connector("postgres", create_mock_connector)
        register_connector("duckdb", create_mock_connector)
        register_connector("filestore", create_mock_connector)

        types = list_connector_types()

        assert "postgres" in types
        assert "duckdb" in types
        assert "filestore" in types

    def test_list_connector_types_empty(self):
        """Test listing connector types when none registered."""
        types = list_connector_types()
        assert types == []


class TestClearRegistries:
    """Tests for clear_registries function."""

    def test_clear_registries(self):
        """Test that clear_registries removes all registrations."""
        register_connector("mock_connector", create_mock_connector)

        clear_registries()

        assert list_connector_types() == []


class TestFactoryPattern:
    """Integration tests for the factory pattern."""

    def test_factory_receives_config(self):
        """Test that factory function receives correct arguments."""
        captured_args: list = []

        def capture_factory(config):
            captured_args.append(config)
            return MockConnector(config)

        config = SourceConfig(
            type="postgres",
            host="prod.db.com",
            database="production",
            user="admin",
            table="orders",
        )

        register_connector("capture", capture_factory)
        get_connector("capture", config)

        assert len(captured_args) == 1
        assert captured_args[0] is config

    def test_full_pipeline_integration(self):
        """Integration test: register connector, create instance, and process data."""
        # Register connector
        register_connector("mock", create_mock_connector)

        # Create configs
        source_config = SourceConfig(
            type="postgres",
            host="localhost",
            database="test",
            user="user",
            table="users",
        )
        dest_config = DestinationConfig(
            type="duckdb",
            host="localhost",
            database="test.db",
            user="user",
            table="output",
        )

        # Get connector instances (same connector can be used for both source and destination)
        source = get_connector("mock", source_config)
        destination = get_connector("mock", dest_config)
        state = State()

        # Process data through pipeline
        for batch in source.read_batches(state):
            destination.write_batch(batch, state)

        # Verify data was transferred
        assert len(destination.written_batches) == 1
        assert destination.written_batches[0].columns == ["id", "name"]
