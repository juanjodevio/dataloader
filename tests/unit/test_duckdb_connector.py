"""Unit tests for DuckDBConnector."""

import tempfile
from pathlib import Path

import pytest

from dataloader.connectors.duckdb.connector import (
    DuckDBConnector,
    create_duckdb_connector,
)
from dataloader.core.batch import ArrowBatch
from dataloader.core.exceptions import ConnectorError
from dataloader.core.state import State
from dataloader.models.destination_config import DestinationConfig
from dataloader.models.source_config import SourceConfig


class TestDuckDBConnector:
    """Tests for DuckDBConnector."""

    @pytest.fixture
    def duckdb_config(self) -> DestinationConfig:
        """Create a DuckDB destination config."""
        return DestinationConfig(
            type="duckdb",
            database=":memory:",
            table="test_table",
        )

    @pytest.fixture
    def sample_batch(self) -> ArrowBatch:
        """Create a sample batch with typed metadata."""
        return ArrowBatch.from_rows(
            columns=["id", "name", "score"],
            rows=[
                [1, "Alice", 95.5],
                [2, "Bob", 87.0],
                [3, "Charlie", 92.3],
            ],
            metadata={
                "column_types": {"id": "int", "name": "string", "score": "float"},
                "batch_number": 1,
            },
        )

    @pytest.fixture
    def file_based_config(self, tmp_path: Path) -> DestinationConfig:
        """Create a DuckDB config with file-based database."""
        db_path = tmp_path / "test.duckdb"
        return DestinationConfig(
            type="duckdb",
            database=str(db_path),
            table="users",
        )

    def test_duckdb_connector_initialization(self, duckdb_config: DestinationConfig):
        """Test that DuckDBConnector initializes correctly."""
        connector = DuckDBConnector(duckdb_config)

        assert connector._database == ":memory:"
        assert connector._table == "test_table"
        assert connector._conn is None

    def test_duckdb_in_memory_write(
        self, duckdb_config: DestinationConfig, sample_batch: ArrowBatch
    ):
        """Test writing batches to in-memory DuckDB."""
        connector = DuckDBConnector(duckdb_config)
        state = State()

        connector.write_batch(sample_batch, state)

        # Verify data was written
        conn = connector._get_connection()
        result = conn.execute("SELECT * FROM test_table ORDER BY id").fetchall()

        assert len(result) == 3
        assert result[0] == (1, "Alice", 95.5)
        assert result[1] == (2, "Bob", 87.0)
        assert result[2] == (3, "Charlie", 92.3)

        connector.close()

    def test_duckdb_file_based_write(
        self, file_based_config: DestinationConfig, sample_batch: ArrowBatch
    ):
        """Test writing to file-based DuckDB database."""
        connector = DuckDBConnector(file_based_config)
        state = State()

        connector.write_batch(sample_batch, state)

        # Verify data was written
        conn = connector._get_connection()
        result = conn.execute("SELECT COUNT(*) FROM users").fetchone()

        assert result[0] == 3

        connector.close()

        # Verify file was created
        assert Path(file_based_config.database).exists()

    def test_duckdb_table_creation(
        self, duckdb_config: DestinationConfig, sample_batch: ArrowBatch
    ):
        """Test that table is created from batch schema."""
        connector = DuckDBConnector(duckdb_config)
        state = State()

        connector.write_batch(sample_batch, state)

        conn = connector._get_connection()
        result = conn.execute("DESCRIBE test_table").fetchall()

        column_info = {row[0]: row[1] for row in result}
        assert "id" in column_info
        assert "name" in column_info
        assert "score" in column_info
        # ArrowBatch uses int64 by default, which maps to BIGINT in DuckDB
        assert "INTEGER" in column_info["id"] or "BIGINT" in column_info["id"]
        assert "VARCHAR" in column_info["name"]
        assert "DOUBLE" in column_info["score"]

        connector.close()

    def test_duckdb_schema_evolution(self, duckdb_config: DestinationConfig):
        """Test that new columns are added to existing table."""
        connector = DuckDBConnector(duckdb_config)
        state = State()

        # First batch
        batch1 = ArrowBatch.from_rows(
            columns=["id", "name"],
            rows=[[1, "Alice"]],
            metadata={"column_types": {"id": "int", "name": "string"}},
        )
        connector.write_batch(batch1, state)

        # Second batch with new column
        batch2 = ArrowBatch.from_rows(
            columns=["id", "name", "age"],
            rows=[[2, "Bob", 25]],
            metadata={"column_types": {"id": "int", "name": "string", "age": "int"}},
        )
        connector.write_batch(batch2, state)

        conn = connector._get_connection()
        result = conn.execute("DESCRIBE test_table").fetchall()
        columns = [row[0] for row in result]

        assert "age" in columns

        connector.close()

    def test_duckdb_append_mode(self, duckdb_config: DestinationConfig):
        """Test append mode adds rows to existing table."""
        connector = DuckDBConnector(duckdb_config)
        state = State()

        batch1 = ArrowBatch.from_rows(
            columns=["id", "name"],
            rows=[[1, "Alice"]],
            metadata={},
        )
        batch2 = ArrowBatch.from_rows(
            columns=["id", "name"],
            rows=[[2, "Bob"]],
            metadata={},
        )

        connector.write_batch(batch1, state)
        connector.write_batch(batch2, state)

        conn = connector._get_connection()
        result = conn.execute("SELECT COUNT(*) FROM test_table").fetchone()

        assert result[0] == 2

        connector.close()

    def test_duckdb_overwrite_mode(self, duckdb_config: DestinationConfig):
        """Test overwrite mode drops and recreates table."""
        duckdb_config.write_mode = "overwrite"
        connector = DuckDBConnector(duckdb_config)
        state = State()

        batch1 = ArrowBatch.from_rows(
            columns=["id", "name"],
            rows=[[1, "Alice"], [2, "Bob"]],
            metadata={},
        )
        connector.write_batch(batch1, state)

        # Recreate connector to simulate new run
        connector2 = DuckDBConnector(duckdb_config)
        connector2._conn = connector._conn  # Share connection for in-memory db

        batch2 = ArrowBatch.from_rows(
            columns=["id", "name"],
            rows=[[3, "Charlie"]],
            metadata={},
        )
        connector2.write_batch(batch2, state)

        conn = connector2._get_connection()
        result = conn.execute("SELECT * FROM test_table").fetchall()

        # Only the new row should exist
        assert len(result) == 1
        assert result[0] == (3, "Charlie")

        connector.close()

    def test_duckdb_merge_mode_raises_error(
        self, duckdb_config: DestinationConfig, sample_batch: ArrowBatch
    ):
        """Test that merge mode raises ConnectorError."""
        duckdb_config.write_mode = "merge"
        duckdb_config.merge_keys = ["id"]
        connector = DuckDBConnector(duckdb_config)
        state = State()

        with pytest.raises(ConnectorError) as exc_info:
            connector.write_batch(sample_batch, state)

        assert "Merge write mode is not supported" in str(exc_info.value)
        connector.close()

    def test_duckdb_empty_batch(self, duckdb_config: DestinationConfig):
        """Test that empty batch doesn't cause errors."""
        connector = DuckDBConnector(duckdb_config)
        state = State()

        batch = ArrowBatch.from_rows(
            columns=["id", "name"],
            rows=[],
            metadata={},
        )
        connector.write_batch(batch, state)

        conn = connector._get_connection()
        result = conn.execute("SELECT COUNT(*) FROM test_table").fetchone()

        assert result[0] == 0

        connector.close()

    def test_duckdb_with_schema(self):
        """Test writing to table with schema prefix."""
        config = DestinationConfig(
            type="duckdb",
            database=":memory:",
            table="users",
            db_schema="main",
        )
        connector = DuckDBConnector(config)

        assert connector._qualified_table == '"main"."users"'

        connector.close()

    def test_duckdb_type_mapping(self, duckdb_config: DestinationConfig):
        """Test type mapping from Arrow types to DuckDB types."""
        import pyarrow as pa

        from dataloader.connectors.duckdb.type_mapper import DuckDBTypeMapper

        mapper = DuckDBTypeMapper()

        # Test Arrow to DuckDB type mapping
        assert mapper.arrow_to_connector_type(pa.string()) == "VARCHAR"
        assert mapper.arrow_to_connector_type(pa.int64()) == "BIGINT"
        assert mapper.arrow_to_connector_type(pa.int32()) == "INTEGER"
        assert mapper.arrow_to_connector_type(pa.float64()) == "DOUBLE"
        assert mapper.arrow_to_connector_type(pa.float32()) == "FLOAT"
        assert mapper.arrow_to_connector_type(pa.bool_()) == "BOOLEAN"
        assert mapper.arrow_to_connector_type(pa.timestamp("us")) == "TIMESTAMP"
        assert mapper.arrow_to_connector_type(pa.date32()) == "DATE"

        # Test DuckDB to Arrow type mapping
        assert mapper.connector_type_to_arrow("VARCHAR").equals(pa.string())
        assert mapper.connector_type_to_arrow("BIGINT").equals(pa.int64())
        assert mapper.connector_type_to_arrow("INTEGER").equals(pa.int32())
        assert mapper.connector_type_to_arrow("DOUBLE").equals(pa.float64())
        assert mapper.connector_type_to_arrow("BOOLEAN").equals(pa.bool_())
        assert mapper.connector_type_to_arrow("TIMESTAMP").equals(pa.timestamp("us"))
        assert mapper.connector_type_to_arrow("DATE").equals(pa.date32())

    def test_create_duckdb_connector_factory(self, duckdb_config: DestinationConfig):
        """Test the factory function creates DuckDBConnector."""
        connector = create_duckdb_connector(duckdb_config)
        assert isinstance(connector, DuckDBConnector)
        connector.close()

    def test_duckdb_full_refresh_overwrite_drops_table(
        self, duckdb_config: DestinationConfig, sample_batch: ArrowBatch
    ):
        """Test that full_refresh=True with overwrite mode drops and recreates table."""
        duckdb_config.write_mode = "overwrite"
        connector = DuckDBConnector(duckdb_config)
        state = State(metadata={"full_refresh": True})

        # Write first batch - should drop and create
        connector.write_batch(sample_batch, state)

        conn = connector._get_connection()
        result = conn.execute("SELECT * FROM test_table ORDER BY id").fetchall()

        assert len(result) == 3
        assert result[0] == (1, "Alice", 95.5)

        # Write second batch - should NOT drop again (only inserts)
        batch2 = ArrowBatch.from_rows(
            columns=["id", "name", "score"],
            rows=[[4, "David", 88.0]],
            metadata={},
        )
        connector.write_batch(batch2, state)

        result = conn.execute("SELECT * FROM test_table ORDER BY id").fetchall()
        # Should have all 4 rows (3 from first batch + 1 from second)
        assert len(result) == 4
        assert result[3] == (4, "David", 88.0)

        connector.close()

    def test_duckdb_default_overwrite_deletes_rows(
        self, duckdb_config: DestinationConfig, sample_batch: ArrowBatch
    ):
        """Test that full_refresh=False with overwrite mode deletes rows (not drops table)."""
        duckdb_config.write_mode = "overwrite"
        connector = DuckDBConnector(duckdb_config)
        state = State(metadata={"full_refresh": False})

        # Write first batch - creates table and inserts
        connector.write_batch(sample_batch, state)

        conn = connector._get_connection()
        result = conn.execute("SELECT COUNT(*) FROM test_table").fetchone()
        assert result[0] == 3

        # Verify table structure exists
        describe_result = conn.execute("DESCRIBE test_table").fetchall()
        columns = [row[0] for row in describe_result]
        assert "id" in columns
        assert "name" in columns
        assert "score" in columns

        # Recreate connector to simulate new run with overwrite
        connector2 = DuckDBConnector(duckdb_config)
        connector2._conn = connector._conn  # Share connection for in-memory db
        state2 = State(metadata={"full_refresh": False})

        batch2 = ArrowBatch.from_rows(
            columns=["id", "name", "score"],
            rows=[[5, "Eve", 90.0]],
            metadata={},
        )
        # Should DELETE existing rows and insert new ones
        connector2.write_batch(batch2, state2)

        result = conn.execute("SELECT * FROM test_table").fetchall()
        # Should only have the new row
        assert len(result) == 1
        assert result[0] == (5, "Eve", 90.0)

        # Table structure should still exist
        describe_result2 = conn.execute("DESCRIBE test_table").fetchall()
        columns2 = [row[0] for row in describe_result2]
        assert columns == columns2  # Structure preserved

        connector.close()

    def test_duckdb_full_refresh_append_drops_table(
        self, duckdb_config: DestinationConfig, sample_batch: ArrowBatch
    ):
        """Test that full_refresh=True with append mode drops and recreates table."""
        duckdb_config.write_mode = "append"
        connector = DuckDBConnector(duckdb_config)
        state = State(metadata={"full_refresh": True})

        connector.write_batch(sample_batch, state)

        conn = connector._get_connection()
        result = conn.execute("SELECT * FROM test_table ORDER BY id").fetchall()

        assert len(result) == 3
        assert result[0] == (1, "Alice", 95.5)

        connector.close()

    def test_duckdb_full_refresh_only_drops_once(
        self, duckdb_config: DestinationConfig, sample_batch: ArrowBatch
    ):
        """Test that full_refresh only drops table on first batch, not subsequent batches."""
        duckdb_config.write_mode = "overwrite"
        connector = DuckDBConnector(duckdb_config)
        state = State(metadata={"full_refresh": True})

        # Write first batch
        connector.write_batch(sample_batch, state)

        conn = connector._get_connection()
        result = conn.execute("SELECT COUNT(*) FROM test_table").fetchone()
        assert result[0] == 3

        # Write second batch - should NOT drop again
        batch2 = ArrowBatch.from_rows(
            columns=["id", "name", "score"],
            rows=[[4, "David", 88.0], [5, "Eve", 90.0]],
            metadata={},
        )
        connector.write_batch(batch2, state)

        # Should have all rows (3 from first + 2 from second = 5 total)
        result = conn.execute("SELECT COUNT(*) FROM test_table").fetchone()
        assert result[0] == 5

        connector.close()


class TestDuckDBConnectorRegistration:
    """Tests for DuckDBConnector registration."""

    @pytest.fixture(autouse=True)
    def ensure_registration(self):
        """Ensure built-in connectors are registered before each test."""
        from dataloader.connectors import reregister_builtins

        reregister_builtins()

    def test_connectors_registered(self):
        """Test that built-in connectors are registered."""
        from dataloader.connectors import list_connector_types

        types = list_connector_types()
        assert "duckdb" in types
        assert "postgres" in types
        assert "filestore" in types

    def test_get_connector_duckdb(self):
        """Test getting DuckDBConnector via registry."""
        from dataloader.connectors import get_connector

        config = DestinationConfig(
            type="duckdb",
            database=":memory:",
            table="test",
        )
        connector = get_connector("duckdb", config)

        assert isinstance(connector, DuckDBConnector)
        connector.close()

    def test_get_unknown_connector_raises_error(self):
        """Test that unknown connector type raises ConnectorError."""
        from dataloader.connectors import get_connector

        config = DestinationConfig(
            type="duckdb",  # Valid type for config validation
            database=":memory:",
            table="test",
        )

        with pytest.raises(ConnectorError) as exc_info:
            get_connector("unknown", config)

        assert "Unknown connector type" in str(exc_info.value)


class TestDuckDBIntegration:
    """Integration tests for DuckDB with real database operations."""

    def test_full_pipeline_in_memory(self):
        """Test a complete write pipeline with in-memory DuckDB."""
        config = DestinationConfig(
            type="duckdb",
            database=":memory:",
            table="events",
        )
        connector = DuckDBConnector(config)
        state = State()

        # Write multiple batches
        for i in range(3):
            batch = ArrowBatch.from_rows(
                columns=["event_id", "event_type", "timestamp"],
                rows=[
                    [i * 2, "click", f"2024-01-{i + 1:02d}"],
                    [i * 2 + 1, "view", f"2024-01-{i + 1:02d}"],
                ],
                metadata={
                    "column_types": {
                        "event_id": "int",
                        "event_type": "string",
                        "timestamp": "string",
                    }
                },
            )
            connector.write_batch(batch, state)

        # Verify all data
        conn = connector._get_connection()
        result = conn.execute("SELECT COUNT(*) FROM events").fetchone()
        assert result[0] == 6

        # Verify ordering
        result = conn.execute(
            "SELECT event_id FROM events ORDER BY event_id"
        ).fetchall()
        assert [r[0] for r in result] == [0, 1, 2, 3, 4, 5]

        connector.close()

    def test_full_pipeline_file_based(self, tmp_path: Path):
        """Test a complete write pipeline with file-based DuckDB."""
        db_path = tmp_path / "pipeline.duckdb"
        config = DestinationConfig(
            type="duckdb",
            database=str(db_path),
            table="metrics",
        )

        # First run: write data
        connector1 = DuckDBConnector(config)
        state = State()

        batch = ArrowBatch.from_rows(
            columns=["metric", "value"],
            rows=[["cpu", 75.5], ["memory", 80.2]],
            metadata={},
        )
        connector1.write_batch(batch, state)
        connector1.close()

        # Second run: append more data
        connector2 = DuckDBConnector(config)
        batch2 = ArrowBatch.from_rows(
            columns=["metric", "value"],
            rows=[["disk", 45.0]],
            metadata={},
        )
        connector2.write_batch(batch2, state)

        # Verify all data persisted
        conn = connector2._get_connection()
        result = conn.execute("SELECT COUNT(*) FROM metrics").fetchone()
        assert result[0] == 3

        connector2.close()
