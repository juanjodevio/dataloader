"""Tests for connector protocols and registry."""

from typing import Iterable

import pytest

from dataloader.connectors import (
    Destination,
    Source,
    clear_registries,
    get_destination,
    get_source,
    list_destination_types,
    list_source_types,
    register_destination,
    register_source,
)
from dataloader.core.batch import Batch, DictBatch
from dataloader.core.exceptions import ConnectorError
from dataloader.core.state import State
from dataloader.models.destination_config import DestinationConfig
from dataloader.models.source_config import SourceConfig


class MockSource:
    """Mock source implementation for testing."""

    def __init__(self, config: SourceConfig, connection: dict):
        self.config = config
        self.connection = connection

    def read_batches(self, state: State) -> Iterable[Batch]:
        yield DictBatch(
            columns=["id", "name"],
            rows=[[1, "test"]],
            metadata={"source": "mock"},
        )


class MockDestination:
    """Mock destination implementation for testing."""

    def __init__(self, config: DestinationConfig, connection: dict):
        self.config = config
        self.connection = connection
        self.written_batches: list[Batch] = []

    def write_batch(self, batch: Batch, state: State) -> None:
        self.written_batches.append(batch)


@pytest.fixture(autouse=True)
def clean_registry():
    """Clear registries before and after each test."""
    clear_registries()
    yield
    clear_registries()


class TestSourceProtocol:
    """Tests for Source protocol compliance."""

    def test_mock_source_satisfies_protocol(self):
        """Verify MockSource implements the Source protocol."""
        config = SourceConfig(
            type="postgres",
            host="localhost",
            database="test",
            user="user",
            table="users",
        )
        source = MockSource(config, {"host": "localhost"})

        assert isinstance(source, Source)

    def test_mock_source_read_batches(self):
        """Test that read_batches yields valid batches."""
        config = SourceConfig(
            type="postgres",
            host="localhost",
            database="test",
            user="user",
            table="users",
        )
        source = MockSource(config, {})
        state = State()

        batches = list(source.read_batches(state))

        assert len(batches) == 1
        assert batches[0].columns == ["id", "name"]
        assert batches[0].rows == [[1, "test"]]


class TestDestinationProtocol:
    """Tests for Destination protocol compliance."""

    def test_mock_destination_satisfies_protocol(self):
        """Verify MockDestination implements the Destination protocol."""
        config = DestinationConfig(
            type="duckdb",
            host="localhost",
            database="test.db",
            user="user",
            table="output",
        )
        destination = MockDestination(config, {})

        assert isinstance(destination, Destination)

    def test_mock_destination_write_batch(self):
        """Test that write_batch accepts valid batches."""
        config = DestinationConfig(
            type="duckdb",
            host="localhost",
            database="test.db",
            user="user",
            table="output",
        )
        destination = MockDestination(config, {})
        state = State()
        batch = DictBatch(columns=["id"], rows=[[1]])

        destination.write_batch(batch, state)

        assert len(destination.written_batches) == 1
        assert destination.written_batches[0] is batch


class TestSourceRegistry:
    """Tests for source registry functions."""

    def test_register_and_get_source(self):
        """Test registering and retrieving a source factory."""
        config = SourceConfig(
            type="postgres",
            host="localhost",
            database="test",
            user="user",
            table="users",
        )
        connection = {"host": "localhost", "port": 5432}

        register_source("mock", MockSource)

        source = get_source("mock", config, connection)

        assert isinstance(source, MockSource)
        assert source.config is config
        assert source.connection is connection

    def test_register_duplicate_source_raises(self):
        """Test that registering duplicate source type raises ConnectorError."""
        register_source("mock", MockSource)

        with pytest.raises(ConnectorError) as exc_info:
            register_source("mock", MockSource)

        assert "already registered" in str(exc_info.value)
        assert exc_info.value.context["connector_type"] == "mock"

    def test_get_unknown_source_raises(self):
        """Test that getting unknown source type raises ConnectorError."""
        config = SourceConfig(
            type="postgres",
            host="localhost",
            database="test",
            user="user",
            table="users",
        )

        with pytest.raises(ConnectorError) as exc_info:
            get_source("unknown", config, {})

        assert "Unknown source connector type" in str(exc_info.value)
        assert exc_info.value.context["connector_type"] == "unknown"

    def test_get_unknown_source_shows_available(self):
        """Test that error message includes available connector types."""
        register_source("csv", MockSource)
        register_source("postgres", MockSource)
        config = SourceConfig(type="csv", path="/tmp/test.csv")

        with pytest.raises(ConnectorError) as exc_info:
            get_source("unknown", config, {})

        assert "csv, postgres" in exc_info.value.context["available_types"]

    def test_list_source_types(self):
        """Test listing registered source types."""
        register_source("postgres", MockSource)
        register_source("csv", MockSource)
        register_source("s3", MockSource)

        types = list_source_types()

        assert types == ["csv", "postgres", "s3"]

    def test_list_source_types_empty(self):
        """Test listing source types when none registered."""
        types = list_source_types()
        assert types == []


class TestDestinationRegistry:
    """Tests for destination registry functions."""

    def test_register_and_get_destination(self):
        """Test registering and retrieving a destination factory."""
        config = DestinationConfig(
            type="duckdb",
            host="localhost",
            database="test.db",
            user="user",
            table="output",
        )
        connection = {"database": "test.db"}

        register_destination("mock", MockDestination)

        destination = get_destination("mock", config, connection)

        assert isinstance(destination, MockDestination)
        assert destination.config is config
        assert destination.connection is connection

    def test_register_duplicate_destination_raises(self):
        """Test that registering duplicate destination type raises ConnectorError."""
        register_destination("mock", MockDestination)

        with pytest.raises(ConnectorError) as exc_info:
            register_destination("mock", MockDestination)

        assert "already registered" in str(exc_info.value)
        assert exc_info.value.context["connector_type"] == "mock"

    def test_get_unknown_destination_raises(self):
        """Test that getting unknown destination type raises ConnectorError."""
        config = DestinationConfig(
            type="duckdb",
            host="localhost",
            database="test.db",
            user="user",
            table="output",
        )

        with pytest.raises(ConnectorError) as exc_info:
            get_destination("unknown", config, {})

        assert "Unknown destination connector type" in str(exc_info.value)
        assert exc_info.value.context["connector_type"] == "unknown"

    def test_get_unknown_destination_shows_available(self):
        """Test that error message includes available connector types."""
        register_destination("duckdb", MockDestination)
        register_destination("s3", MockDestination)
        config = DestinationConfig(
            type="duckdb",
            host="localhost",
            database="test.db",
            user="user",
            table="output",
        )

        with pytest.raises(ConnectorError) as exc_info:
            get_destination("unknown", config, {})

        assert "duckdb, s3" in exc_info.value.context["available_types"]

    def test_list_destination_types(self):
        """Test listing registered destination types."""
        register_destination("duckdb", MockDestination)
        register_destination("s3", MockDestination)

        types = list_destination_types()

        assert types == ["duckdb", "s3"]

    def test_list_destination_types_empty(self):
        """Test listing destination types when none registered."""
        types = list_destination_types()
        assert types == []


class TestClearRegistries:
    """Tests for clear_registries function."""

    def test_clear_registries(self):
        """Test that clear_registries removes all registrations."""
        register_source("mock_source", MockSource)
        register_destination("mock_dest", MockDestination)

        clear_registries()

        assert list_source_types() == []
        assert list_destination_types() == []


class TestFactoryPattern:
    """Integration tests for the factory pattern."""

    def test_factory_receives_config_and_connection(self):
        """Test that factory function receives correct arguments."""
        captured_args: list = []

        def capture_factory(config, connection):
            captured_args.append((config, connection))
            return MockSource(config, connection)

        config = SourceConfig(
            type="postgres",
            host="prod.db.com",
            database="production",
            user="admin",
            table="orders",
        )
        connection = {"password": "secret", "ssl": True}

        register_source("capture", capture_factory)
        get_source("capture", config, connection)

        assert len(captured_args) == 1
        assert captured_args[0][0] is config
        assert captured_args[0][1] is connection

    def test_full_pipeline_integration(self):
        """Integration test: register connectors, create instances, and process data."""
        # Register both connectors
        register_source("mock", MockSource)
        register_destination("mock", MockDestination)

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

        # Get connector instances
        source = get_source("mock", source_config, {})
        destination = get_destination("mock", dest_config, {})
        state = State()

        # Process data through pipeline
        for batch in source.read_batches(state):
            destination.write_batch(batch, state)

        # Verify data was transferred
        assert len(destination.written_batches) == 1
        assert destination.written_batches[0].columns == ["id", "name"]

