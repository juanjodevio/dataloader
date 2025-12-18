"""Unit tests for destination connectors (DuckDB, S3)."""

import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from dataloader.connectors.duckdb.destination import (
    DuckDBDestination,
    create_duckdb_destination,
)
from dataloader.connectors.s3.destination import S3Destination, create_s3_destination
from dataloader.core.batch import DictBatch
from dataloader.core.exceptions import ConnectorError
from dataloader.core.state import State
from dataloader.models.destination_config import DestinationConfig


class TestDuckDBDestination:
    """Tests for DuckDBDestination connector."""

    @pytest.fixture
    def duckdb_config(self) -> DestinationConfig:
        """Create a DuckDB destination config."""
        return DestinationConfig(
            type="duckdb",
            database=":memory:",
            table="test_table",
        )

    @pytest.fixture
    def sample_batch(self) -> DictBatch:
        """Create a sample batch with typed metadata."""
        return DictBatch(
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

    def test_duckdb_destination_initialization(self, duckdb_config: DestinationConfig):
        """Test that DuckDBDestination initializes correctly."""
        connection = {"database": "/custom/path.duckdb"}
        dest = DuckDBDestination(duckdb_config, connection)

        assert dest._database == "/custom/path.duckdb"
        assert dest._table == "test_table"
        assert dest._conn is None

    def test_duckdb_in_memory_write(
        self, duckdb_config: DestinationConfig, sample_batch: DictBatch
    ):
        """Test writing batches to in-memory DuckDB."""
        dest = DuckDBDestination(duckdb_config, {})
        state = State()

        dest.write_batch(sample_batch, state)

        # Verify data was written
        conn = dest._get_connection()
        result = conn.execute("SELECT * FROM test_table ORDER BY id").fetchall()

        assert len(result) == 3
        assert result[0] == (1, "Alice", 95.5)
        assert result[1] == (2, "Bob", 87.0)
        assert result[2] == (3, "Charlie", 92.3)

        dest.close()

    def test_duckdb_file_based_write(
        self, file_based_config: DestinationConfig, sample_batch: DictBatch
    ):
        """Test writing to file-based DuckDB database."""
        dest = DuckDBDestination(file_based_config, {})
        state = State()

        dest.write_batch(sample_batch, state)

        # Verify data was written
        conn = dest._get_connection()
        result = conn.execute("SELECT COUNT(*) FROM users").fetchone()

        assert result[0] == 3

        dest.close()

        # Verify file was created
        assert Path(file_based_config.database).exists()

    def test_duckdb_table_creation(
        self, duckdb_config: DestinationConfig, sample_batch: DictBatch
    ):
        """Test that table is created from batch schema."""
        dest = DuckDBDestination(duckdb_config, {})
        state = State()

        dest.write_batch(sample_batch, state)

        conn = dest._get_connection()
        result = conn.execute("DESCRIBE test_table").fetchall()

        column_info = {row[0]: row[1] for row in result}
        assert "id" in column_info
        assert "name" in column_info
        assert "score" in column_info
        assert "INTEGER" in column_info["id"]
        assert "VARCHAR" in column_info["name"]
        assert "DOUBLE" in column_info["score"]

        dest.close()

    def test_duckdb_schema_evolution(self, duckdb_config: DestinationConfig):
        """Test that new columns are added to existing table."""
        dest = DuckDBDestination(duckdb_config, {})
        state = State()

        # First batch
        batch1 = DictBatch(
            columns=["id", "name"],
            rows=[[1, "Alice"]],
            metadata={"column_types": {"id": "int", "name": "string"}},
        )
        dest.write_batch(batch1, state)

        # Second batch with new column
        batch2 = DictBatch(
            columns=["id", "name", "age"],
            rows=[[2, "Bob", 25]],
            metadata={"column_types": {"id": "int", "name": "string", "age": "int"}},
        )
        dest.write_batch(batch2, state)

        conn = dest._get_connection()
        result = conn.execute("DESCRIBE test_table").fetchall()
        columns = [row[0] for row in result]

        assert "age" in columns

        dest.close()

    def test_duckdb_append_mode(self, duckdb_config: DestinationConfig):
        """Test append mode adds rows to existing table."""
        dest = DuckDBDestination(duckdb_config, {})
        state = State()

        batch1 = DictBatch(
            columns=["id", "name"],
            rows=[[1, "Alice"]],
            metadata={},
        )
        batch2 = DictBatch(
            columns=["id", "name"],
            rows=[[2, "Bob"]],
            metadata={},
        )

        dest.write_batch(batch1, state)
        dest.write_batch(batch2, state)

        conn = dest._get_connection()
        result = conn.execute("SELECT COUNT(*) FROM test_table").fetchone()

        assert result[0] == 2

        dest.close()

    def test_duckdb_overwrite_mode(self, duckdb_config: DestinationConfig):
        """Test overwrite mode drops and recreates table."""
        duckdb_config.write_mode = "overwrite"
        dest = DuckDBDestination(duckdb_config, {})
        state = State()

        batch1 = DictBatch(
            columns=["id", "name"],
            rows=[[1, "Alice"], [2, "Bob"]],
            metadata={},
        )
        dest.write_batch(batch1, state)

        # Recreate destination to simulate new run
        dest2 = DuckDBDestination(duckdb_config, {})
        dest2._conn = dest._conn  # Share connection for in-memory db

        batch2 = DictBatch(
            columns=["id", "name"],
            rows=[[3, "Charlie"]],
            metadata={},
        )
        dest2.write_batch(batch2, state)

        conn = dest2._get_connection()
        result = conn.execute("SELECT * FROM test_table").fetchall()

        # Only the new row should exist
        assert len(result) == 1
        assert result[0] == (3, "Charlie")

        dest.close()

    def test_duckdb_merge_mode_raises_error(
        self, duckdb_config: DestinationConfig, sample_batch: DictBatch
    ):
        """Test that merge mode raises ConnectorError."""
        duckdb_config.write_mode = "merge"
        duckdb_config.merge_keys = ["id"]
        dest = DuckDBDestination(duckdb_config, {})
        state = State()

        with pytest.raises(ConnectorError) as exc_info:
            dest.write_batch(sample_batch, state)

        assert "Merge write mode is not supported" in str(exc_info.value)
        dest.close()

    def test_duckdb_empty_batch(self, duckdb_config: DestinationConfig):
        """Test that empty batch doesn't cause errors."""
        dest = DuckDBDestination(duckdb_config, {})
        state = State()

        batch = DictBatch(
            columns=["id", "name"],
            rows=[],
            metadata={},
        )
        dest.write_batch(batch, state)

        conn = dest._get_connection()
        result = conn.execute("SELECT COUNT(*) FROM test_table").fetchone()

        assert result[0] == 0

        dest.close()

    def test_duckdb_with_schema(self):
        """Test writing to table with schema prefix."""
        config = DestinationConfig(
            type="duckdb",
            database=":memory:",
            table="users",
            db_schema="main",
        )
        dest = DuckDBDestination(config, {})

        assert dest._qualified_table == '"main"."users"'

        dest.close()

    def test_duckdb_type_mapping(self, duckdb_config: DestinationConfig):
        """Test type mapping from batch types to DuckDB types."""
        dest = DuckDBDestination(duckdb_config, {})

        assert dest._map_type("string") == "VARCHAR"
        assert dest._map_type("int") == "INTEGER"
        assert dest._map_type("float") == "DOUBLE"
        assert dest._map_type("datetime") == "TIMESTAMP"
        assert dest._map_type("bool") == "BOOLEAN"
        assert dest._map_type("unknown") == "VARCHAR"

        dest.close()

    def test_duckdb_infer_column_type(self, duckdb_config: DestinationConfig):
        """Test type inference from Python values."""
        dest = DuckDBDestination(duckdb_config, {})

        assert dest._infer_column_type(None) == "VARCHAR"
        assert dest._infer_column_type(True) == "BOOLEAN"
        assert dest._infer_column_type(42) == "INTEGER"
        assert dest._infer_column_type(3.14) == "DOUBLE"
        assert dest._infer_column_type("hello") == "VARCHAR"

        dest.close()

    def test_create_duckdb_destination_factory(self, duckdb_config: DestinationConfig):
        """Test the factory function creates DuckDBDestination."""
        dest = create_duckdb_destination(duckdb_config, {})
        assert isinstance(dest, DuckDBDestination)
        dest.close()


class TestS3Destination:
    """Tests for S3Destination connector."""

    @pytest.fixture
    def s3_config(self) -> DestinationConfig:
        """Create an S3 destination config."""
        return DestinationConfig(
            type="s3",
            bucket="test-bucket",
            path="output/data",
            access_key="AKIAIOSFODNN7EXAMPLE",
            secret_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            region="us-east-1",
        )

    @pytest.fixture
    def sample_batch(self) -> DictBatch:
        """Create a sample batch."""
        return DictBatch(
            columns=["id", "name", "score"],
            rows=[
                [1, "Alice", 95.5],
                [2, "Bob", 87.0],
            ],
            metadata={"batch_number": 1},
        )

    def test_s3_destination_initialization(self, s3_config: DestinationConfig):
        """Test that S3Destination initializes correctly."""
        connection = {"endpoint_url": "http://localhost:4566", "partition_key": "date"}
        dest = S3Destination(s3_config, connection)

        assert dest._bucket == "test-bucket"
        assert dest._base_path == "output/data"
        assert dest._partition_key == "date"
        assert dest._boto_config["endpoint_url"] == "http://localhost:4566"
        assert dest._boto_config["aws_access_key_id"] == "AKIAIOSFODNN7EXAMPLE"

    def test_s3_destination_config_override(self, s3_config: DestinationConfig):
        """Test that connection dict overrides config values."""
        connection = {
            "aws_access_key_id": "OVERRIDE_KEY",
            "aws_secret_access_key": "OVERRIDE_SECRET",
            "region_name": "eu-west-1",
            "bucket": "override-bucket",
        }
        dest = S3Destination(s3_config, connection)

        assert dest._bucket == "override-bucket"
        assert dest._boto_config["aws_access_key_id"] == "OVERRIDE_KEY"
        assert dest._boto_config["aws_secret_access_key"] == "OVERRIDE_SECRET"
        assert dest._boto_config["region_name"] == "eu-west-1"

    def test_s3_batch_to_csv(self, s3_config: DestinationConfig, sample_batch: DictBatch):
        """Test batch to CSV conversion."""
        dest = S3Destination(s3_config, {})
        csv_content = dest._batch_to_csv(sample_batch)

        lines = csv_content.strip().split("\n")
        assert len(lines) == 3  # Header + 2 rows
        assert lines[0] == "id,name,score"
        assert lines[1] == "1,Alice,95.5"
        assert lines[2] == "2,Bob,87.0"

    def test_s3_generate_key_simple(self, s3_config: DestinationConfig, sample_batch: DictBatch):
        """Test key generation without partitioning."""
        dest = S3Destination(s3_config, {"file_prefix": "export"})
        key = dest._generate_key(sample_batch)

        assert key.startswith("output/data/export_")
        assert key.endswith(".csv")
        assert dest._batch_counter == 1

    def test_s3_generate_key_with_partition(self, s3_config: DestinationConfig):
        """Test key generation with partition key."""
        dest = S3Destination(s3_config, {"partition_key": "region"})
        batch = DictBatch(
            columns=["id", "region", "value"],
            rows=[[1, "us-east", 100]],
            metadata={},
        )

        key = dest._generate_key(batch)

        assert "region=us-east" in key

    @patch("dataloader.connectors.s3.destination.boto3.client")
    def test_s3_write_batch_success(
        self,
        mock_boto_client: MagicMock,
        s3_config: DestinationConfig,
        sample_batch: DictBatch,
    ):
        """Test successful batch write to S3."""
        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client

        dest = S3Destination(s3_config, {})
        state = State()

        dest.write_batch(sample_batch, state)

        mock_client.put_object.assert_called_once()
        call_kwargs = mock_client.put_object.call_args.kwargs
        assert call_kwargs["Bucket"] == "test-bucket"
        assert call_kwargs["Key"].startswith("output/data/data_")
        assert call_kwargs["ContentType"] == "text/csv"

        assert len(dest.written_files) == 1

    @patch("dataloader.connectors.s3.destination.boto3.client")
    def test_s3_write_batch_empty(
        self, mock_boto_client: MagicMock, s3_config: DestinationConfig
    ):
        """Test that empty batch doesn't upload anything."""
        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client

        dest = S3Destination(s3_config, {})
        state = State()

        batch = DictBatch(columns=["id"], rows=[], metadata={})
        dest.write_batch(batch, state)

        mock_client.put_object.assert_not_called()
        assert len(dest.written_files) == 0

    @patch("dataloader.connectors.s3.destination.boto3.client")
    def test_s3_overwrite_mode_deletes_existing(
        self, mock_boto_client: MagicMock, s3_config: DestinationConfig, sample_batch: DictBatch
    ):
        """Test that overwrite mode deletes existing files first."""
        s3_config.write_mode = "overwrite"

        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client

        # Mock list_objects_v2 paginator
        mock_paginator = MagicMock()
        mock_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {
                "Contents": [
                    {"Key": "output/data/old_file1.csv"},
                    {"Key": "output/data/old_file2.csv"},
                ]
            }
        ]

        dest = S3Destination(s3_config, {})
        state = State()

        dest.write_batch(sample_batch, state)

        # Verify delete was called
        mock_client.delete_objects.assert_called_once()
        delete_call = mock_client.delete_objects.call_args.kwargs
        assert delete_call["Bucket"] == "test-bucket"
        assert len(delete_call["Delete"]["Objects"]) == 2

        # Verify new file was uploaded
        mock_client.put_object.assert_called_once()

    @patch("dataloader.connectors.s3.destination.boto3.client")
    def test_s3_append_mode_no_delete(
        self, mock_boto_client: MagicMock, s3_config: DestinationConfig, sample_batch: DictBatch
    ):
        """Test that append mode doesn't delete existing files."""
        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client

        dest = S3Destination(s3_config, {})
        state = State()

        dest.write_batch(sample_batch, state)

        mock_client.delete_objects.assert_not_called()
        mock_client.put_object.assert_called_once()

    def test_s3_merge_mode_raises_error(
        self, s3_config: DestinationConfig, sample_batch: DictBatch
    ):
        """Test that merge mode raises ConnectorError."""
        s3_config.write_mode = "merge"
        s3_config.merge_keys = ["id"]

        dest = S3Destination(s3_config, {})
        state = State()

        with pytest.raises(ConnectorError) as exc_info:
            dest.write_batch(sample_batch, state)

        assert "Merge write mode is not supported for S3" in str(exc_info.value)

    @patch("dataloader.connectors.s3.destination.boto3.client")
    def test_s3_upload_error(
        self, mock_boto_client: MagicMock, s3_config: DestinationConfig, sample_batch: DictBatch
    ):
        """Test that S3 upload errors raise ConnectorError."""
        from botocore.exceptions import ClientError

        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client
        mock_client.put_object.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}},
            "PutObject",
        )

        dest = S3Destination(s3_config, {})
        state = State()

        with pytest.raises(ConnectorError) as exc_info:
            dest.write_batch(sample_batch, state)

        assert "Failed to upload to S3" in str(exc_info.value)
        assert exc_info.value.context["error_code"] == "AccessDenied"

    @patch("dataloader.connectors.s3.destination.boto3.client")
    def test_s3_multiple_batches(
        self, mock_boto_client: MagicMock, s3_config: DestinationConfig
    ):
        """Test writing multiple batches creates unique files."""
        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client

        dest = S3Destination(s3_config, {})
        state = State()

        batch1 = DictBatch(columns=["id"], rows=[[1]], metadata={})
        batch2 = DictBatch(columns=["id"], rows=[[2]], metadata={})

        dest.write_batch(batch1, state)
        dest.write_batch(batch2, state)

        assert mock_client.put_object.call_count == 2
        assert len(dest.written_files) == 2
        assert dest.written_files[0] != dest.written_files[1]

    def test_create_s3_destination_factory(self, s3_config: DestinationConfig):
        """Test the factory function creates S3Destination."""
        dest = create_s3_destination(s3_config, {})
        assert isinstance(dest, S3Destination)


class TestDestinationRegistration:
    """Tests for destination connector registration."""

    @pytest.fixture(autouse=True)
    def ensure_registration(self):
        """Ensure built-in connectors are registered before each test."""
        from dataloader.connectors import reregister_builtins

        reregister_builtins()

    def test_destinations_registered(self):
        """Test that built-in destination connectors are registered."""
        from dataloader.connectors import list_destination_types

        types = list_destination_types()
        assert "duckdb" in types
        assert "s3" in types

    def test_get_destination_duckdb(self):
        """Test getting DuckDBDestination via registry."""
        from dataloader.connectors import get_destination

        config = DestinationConfig(
            type="duckdb",
            database=":memory:",
            table="test",
        )
        dest = get_destination("duckdb", config, {})

        assert isinstance(dest, DuckDBDestination)
        dest.close()

    def test_get_destination_s3(self):
        """Test getting S3Destination via registry."""
        from dataloader.connectors import get_destination

        config = DestinationConfig(
            type="s3",
            bucket="test-bucket",
            path="data/",
        )
        dest = get_destination("s3", config, {})

        assert isinstance(dest, S3Destination)

    def test_get_unknown_destination_raises_error(self):
        """Test that unknown destination type raises ConnectorError."""
        from dataloader.connectors import get_destination

        config = DestinationConfig(
            type="s3",  # Valid type for config validation
            bucket="test",
            path="data/",
        )

        with pytest.raises(ConnectorError) as exc_info:
            get_destination("unknown", config, {})

        assert "Unknown destination connector type" in str(exc_info.value)


class TestDuckDBIntegration:
    """Integration tests for DuckDB with real database operations."""

    def test_full_pipeline_in_memory(self):
        """Test a complete write pipeline with in-memory DuckDB."""
        config = DestinationConfig(
            type="duckdb",
            database=":memory:",
            table="events",
        )
        dest = DuckDBDestination(config, {})
        state = State()

        # Write multiple batches
        for i in range(3):
            batch = DictBatch(
                columns=["event_id", "event_type", "timestamp"],
                rows=[
                    [i * 2, "click", f"2024-01-{i+1:02d}"],
                    [i * 2 + 1, "view", f"2024-01-{i+1:02d}"],
                ],
                metadata={"column_types": {"event_id": "int", "event_type": "string", "timestamp": "string"}},
            )
            dest.write_batch(batch, state)

        # Verify all data
        conn = dest._get_connection()
        result = conn.execute("SELECT COUNT(*) FROM events").fetchone()
        assert result[0] == 6

        # Verify ordering
        result = conn.execute("SELECT event_id FROM events ORDER BY event_id").fetchall()
        assert [r[0] for r in result] == [0, 1, 2, 3, 4, 5]

        dest.close()

    def test_full_pipeline_file_based(self, tmp_path: Path):
        """Test a complete write pipeline with file-based DuckDB."""
        db_path = tmp_path / "pipeline.duckdb"
        config = DestinationConfig(
            type="duckdb",
            database=str(db_path),
            table="metrics",
        )

        # First run: write data
        dest1 = DuckDBDestination(config, {})
        state = State()

        batch = DictBatch(
            columns=["metric", "value"],
            rows=[["cpu", 75.5], ["memory", 80.2]],
            metadata={},
        )
        dest1.write_batch(batch, state)
        dest1.close()

        # Second run: append more data
        dest2 = DuckDBDestination(config, {})
        batch2 = DictBatch(
            columns=["metric", "value"],
            rows=[["disk", 45.0]],
            metadata={},
        )
        dest2.write_batch(batch2, state)

        # Verify all data persisted
        conn = dest2._get_connection()
        result = conn.execute("SELECT COUNT(*) FROM metrics").fetchone()
        assert result[0] == 3

        dest2.close()

