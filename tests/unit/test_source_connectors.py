"""Unit tests for source connectors (Postgres, CSV, S3)."""

import csv
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from dataloader.connectors.csv.source import CSVSource, create_csv_source
from dataloader.connectors.postgres.source import PostgresSource, create_postgres_source
from dataloader.connectors.s3.source import S3Source, create_s3_source
from dataloader.core.batch import DictBatch
from dataloader.core.exceptions import ConnectorError
from dataloader.core.state import State
from dataloader.models.source_config import IncrementalConfig, SourceConfig


class TestPostgresSource:
    """Tests for PostgresSource connector."""

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

    def test_postgres_source_initialization(self, postgres_config: SourceConfig):
        """Test that PostgresSource initializes correctly."""
        connection = {"host": "override-host", "batch_size": 500}
        source = PostgresSource(postgres_config, connection)

        assert source._batch_size == 500
        assert source._connection_params["host"] == "override-host"
        assert source._connection_params["dbname"] == "testdb"
        assert source._conn is None

    def test_postgres_source_uses_config_defaults(self, postgres_config: SourceConfig):
        """Test that PostgresSource falls back to config values."""
        connection: dict[str, Any] = {}
        source = PostgresSource(postgres_config, connection)

        assert source._connection_params["host"] == "localhost"
        assert source._connection_params["port"] == 5432
        assert source._connection_params["dbname"] == "testdb"
        assert source._connection_params["user"] == "testuser"

    def test_postgres_source_default_port(self, postgres_config: SourceConfig):
        """Test that default port is used when not specified."""
        postgres_config.port = None
        source = PostgresSource(postgres_config, {})

        assert source._connection_params["port"] == PostgresSource.DEFAULT_PORT

    @patch("dataloader.connectors.postgres.source.psycopg2.connect")
    def test_postgres_connection_error(
        self, mock_connect: MagicMock, postgres_config: SourceConfig
    ):
        """Test that connection errors raise ConnectorError."""
        import psycopg2

        mock_connect.side_effect = psycopg2.Error("Connection refused")
        source = PostgresSource(postgres_config, {})

        with pytest.raises(ConnectorError) as exc_info:
            source._connect()

        assert "Failed to connect to PostgreSQL" in str(exc_info.value)
        assert exc_info.value.context["host"] == "localhost"

    @patch("dataloader.connectors.postgres.source.psycopg2.connect")
    def test_postgres_read_batches(
        self, mock_connect: MagicMock, postgres_config: SourceConfig
    ):
        """Test reading batches from Postgres."""
        # Setup mock connection and cursor
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        # Mock schema query result
        mock_schema_cursor = MagicMock()
        mock_schema_cursor.fetchall.return_value = [
            ("id", "integer"),
            ("name", "varchar"),
        ]
        mock_schema_cursor.__enter__ = MagicMock(return_value=mock_schema_cursor)
        mock_schema_cursor.__exit__ = MagicMock(return_value=False)

        # Mock data cursor result
        mock_data_cursor = MagicMock()
        mock_data_cursor.fetchmany.side_effect = [
            [(1, "Alice"), (2, "Bob")],  # First batch
            [],  # No more data
        ]
        mock_data_cursor.__enter__ = MagicMock(return_value=mock_data_cursor)
        mock_data_cursor.__exit__ = MagicMock(return_value=False)

        # Configure cursor() to return different cursors based on call
        mock_conn.cursor.side_effect = [mock_schema_cursor, mock_data_cursor]

        source = PostgresSource(postgres_config, {"batch_size": 100})
        state = State()

        batches = list(source.read_batches(state))

        assert len(batches) == 1
        assert batches[0].columns == ["id", "name"]
        assert batches[0].rows == [[1, "Alice"], [2, "Bob"]]
        assert batches[0].metadata["source_type"] == "postgres"
        assert batches[0].metadata["table"] == "users"
        mock_conn.close.assert_called_once()

    @patch("dataloader.connectors.postgres.source.psycopg2.connect")
    def test_postgres_incremental_query(
        self, mock_connect: MagicMock, incremental_postgres_config: SourceConfig
    ):
        """Test that incremental loads use cursor filtering."""
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        mock_schema_cursor = MagicMock()
        mock_schema_cursor.fetchall.return_value = [
            ("id", "integer"),
            ("updated_at", "timestamp"),
        ]
        mock_schema_cursor.__enter__ = MagicMock(return_value=mock_schema_cursor)
        mock_schema_cursor.__exit__ = MagicMock(return_value=False)

        mock_data_cursor = MagicMock()
        mock_data_cursor.fetchmany.side_effect = [[], []]
        mock_data_cursor.__enter__ = MagicMock(return_value=mock_data_cursor)
        mock_data_cursor.__exit__ = MagicMock(return_value=False)

        mock_conn.cursor.side_effect = [mock_schema_cursor, mock_data_cursor]

        source = PostgresSource(incremental_postgres_config, {})
        state = State(cursor_values={"updated_at": "2024-01-01"})

        list(source.read_batches(state))

        # Verify the query includes WHERE clause
        execute_call = mock_data_cursor.execute.call_args
        query = execute_call[0][0]
        params = execute_call[0][1]

        assert 'WHERE "updated_at" > %s' in query
        assert 'ORDER BY "updated_at"' in query
        assert params == ["2024-01-01"]

    def test_create_postgres_source_factory(self, postgres_config: SourceConfig):
        """Test the factory function creates PostgresSource."""
        source = create_postgres_source(postgres_config, {})
        assert isinstance(source, PostgresSource)


class TestCSVSource:
    """Tests for CSVSource connector."""

    @pytest.fixture
    def csv_file(self) -> Path:
        """Create a temporary CSV file."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False, newline=""
        ) as f:
            writer = csv.writer(f)
            writer.writerow(["id", "name", "score"])
            writer.writerow(["1", "Alice", "95.5"])
            writer.writerow(["2", "Bob", "87.0"])
            writer.writerow(["3", "Charlie", "92.3"])
            return Path(f.name)

    @pytest.fixture
    def csv_config(self, csv_file: Path) -> SourceConfig:
        """Create a CSV source config."""
        return SourceConfig(type="csv", path=str(csv_file))

    def test_csv_source_initialization(self, csv_config: SourceConfig):
        """Test that CSVSource initializes correctly."""
        connection = {"encoding": "latin-1", "delimiter": ";", "batch_size": 50}
        source = CSVSource(csv_config, connection)

        assert source._encoding == "latin-1"
        assert source._delimiter == ";"
        assert source._batch_size == 50
        assert source._has_header is True

    def test_csv_source_read_batches(self, csv_config: SourceConfig, csv_file: Path):
        """Test reading batches from CSV file."""
        source = CSVSource(csv_config, {"batch_size": 10})
        state = State()

        batches = list(source.read_batches(state))

        assert len(batches) == 1
        assert batches[0].columns == ["id", "name", "score"]
        assert len(batches[0].rows) == 3
        assert batches[0].rows[0] == ["1", "Alice", "95.5"]
        assert batches[0].metadata["source_type"] == "csv"
        assert batches[0].metadata["file_path"] == str(csv_file)

        # Cleanup
        csv_file.unlink()

    def test_csv_source_schema_inference(self, csv_config: SourceConfig, csv_file: Path):
        """Test that schema is inferred from first batch."""
        source = CSVSource(csv_config, {})
        state = State()

        batches = list(source.read_batches(state))

        column_types = batches[0].metadata["column_types"]
        assert column_types["id"] == "int"
        assert column_types["name"] == "string"
        assert column_types["score"] == "float"

        csv_file.unlink()

    def test_csv_source_no_header(self, csv_file: Path):
        """Test reading CSV without header."""
        # Create file without header
        with open(csv_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["1", "Alice", "95.5"])
            writer.writerow(["2", "Bob", "87.0"])

        config = SourceConfig(type="csv", path=str(csv_file))
        source = CSVSource(config, {"has_header": False})
        state = State()

        batches = list(source.read_batches(state))

        assert batches[0].columns == ["col_0", "col_1", "col_2"]
        assert len(batches[0].rows) == 2

        csv_file.unlink()

    def test_csv_source_file_not_found(self):
        """Test that missing file raises ConnectorError."""
        config = SourceConfig(type="csv", path="/nonexistent/file.csv")
        source = CSVSource(config, {})
        state = State()

        with pytest.raises(ConnectorError) as exc_info:
            list(source.read_batches(state))

        assert "CSV file not found" in str(exc_info.value)
        # Path separators vary by OS, check the filename is present
        assert "file.csv" in exc_info.value.context["path"]

    def test_csv_source_empty_file(self):
        """Test that empty file raises ConnectorError."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        ) as f:
            path = Path(f.name)

        config = SourceConfig(type="csv", path=str(path))
        source = CSVSource(config, {})
        state = State()

        with pytest.raises(ConnectorError) as exc_info:
            list(source.read_batches(state))

        assert "CSV file is empty" in str(exc_info.value)
        path.unlink()

    def test_csv_source_incremental_filtering(self, csv_file: Path):
        """Test cursor-based filtering for incremental loads."""
        config = SourceConfig(
            type="csv",
            path=str(csv_file),
            incremental=IncrementalConfig(strategy="cursor", cursor_column="id"),
        )
        source = CSVSource(config, {})
        state = State(cursor_values={"id": "1"})  # Filter rows where id > 1

        batches = list(source.read_batches(state))

        assert len(batches) == 1
        # Should only include rows with id > 1
        assert len(batches[0].rows) == 2
        assert batches[0].rows[0][0] == "2"
        assert batches[0].rows[1][0] == "3"

        csv_file.unlink()

    def test_csv_source_batching(self):
        """Test that large files are read in batches."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False, newline=""
        ) as f:
            writer = csv.writer(f)
            writer.writerow(["id", "value"])
            for i in range(150):
                writer.writerow([str(i), f"value_{i}"])
            path = Path(f.name)

        config = SourceConfig(type="csv", path=str(path))
        source = CSVSource(config, {"batch_size": 50})
        state = State()

        batches = list(source.read_batches(state))

        assert len(batches) == 3
        assert batches[0].row_count == 50
        assert batches[1].row_count == 50
        assert batches[2].row_count == 50

        path.unlink()

    def test_csv_type_inference(self):
        """Test type inference for various values."""
        source = CSVSource(
            SourceConfig(type="csv", path="/dummy"),
            {},
        )

        assert source._infer_type("123") == "int"
        assert source._infer_type("45.67") == "float"
        assert source._infer_type("hello") == "string"
        assert source._infer_type("2024-01-15") == "datetime"
        assert source._infer_type("2024-01-15 10:30:00") == "datetime"
        assert source._infer_type("") == "string"

    def test_create_csv_source_factory(self, csv_config: SourceConfig):
        """Test the factory function creates CSVSource."""
        source = create_csv_source(csv_config, {})
        assert isinstance(source, CSVSource)


class TestS3Source:
    """Tests for S3Source connector."""

    @pytest.fixture
    def s3_config(self) -> SourceConfig:
        """Create an S3 source config."""
        return SourceConfig(
            type="s3",
            bucket="test-bucket",
            path="data/",
            access_key="AKIAIOSFODNN7EXAMPLE",
            secret_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            region="us-east-1",
        )

    def test_s3_source_initialization(self, s3_config: SourceConfig):
        """Test that S3Source initializes correctly."""
        connection = {"batch_size": 500, "endpoint_url": "http://localhost:4566"}
        source = S3Source(s3_config, connection)

        assert source._batch_size == 500
        assert source._client_config["endpoint_url"] == "http://localhost:4566"
        assert source._client_config["aws_access_key_id"] == "AKIAIOSFODNN7EXAMPLE"
        assert source._client_config["region_name"] == "us-east-1"

    def test_s3_source_client_config_from_connection(self, s3_config: SourceConfig):
        """Test that connection dict takes precedence."""
        connection = {
            "aws_access_key_id": "OVERRIDE_KEY",
            "aws_secret_access_key": "OVERRIDE_SECRET",
            "region_name": "eu-west-1",
        }
        source = S3Source(s3_config, connection)

        assert source._client_config["aws_access_key_id"] == "OVERRIDE_KEY"
        assert source._client_config["aws_secret_access_key"] == "OVERRIDE_SECRET"
        assert source._client_config["region_name"] == "eu-west-1"

    @patch("dataloader.connectors.s3.source.boto3.client")
    def test_s3_list_objects(self, mock_boto_client: MagicMock, s3_config: SourceConfig):
        """Test listing objects from S3."""
        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client

        # Mock paginator
        mock_paginator = MagicMock()
        mock_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {
                "Contents": [
                    {"Key": "data/file1.csv", "LastModified": datetime(2024, 1, 1), "Size": 1000},
                    {"Key": "data/file2.csv", "LastModified": datetime(2024, 1, 2), "Size": 2000},
                    {"Key": "data/image.png", "LastModified": datetime(2024, 1, 3), "Size": 500},
                ]
            }
        ]

        source = S3Source(s3_config, {})
        objects = source._list_objects("test-bucket", "data/")

        # Should only include CSV files
        assert len(objects) == 2
        assert objects[0]["Key"] == "data/file1.csv"
        assert objects[1]["Key"] == "data/file2.csv"

    @patch("dataloader.connectors.s3.source.boto3.client")
    def test_s3_list_objects_error(
        self, mock_boto_client: MagicMock, s3_config: SourceConfig
    ):
        """Test that S3 errors raise ConnectorError."""
        from botocore.exceptions import ClientError

        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client

        mock_paginator = MagicMock()
        mock_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}},
            "ListObjectsV2",
        )

        source = S3Source(s3_config, {})

        with pytest.raises(ConnectorError) as exc_info:
            source._list_objects("test-bucket", "data/")

        assert "Failed to list S3 objects" in str(exc_info.value)
        assert exc_info.value.context["error_code"] == "AccessDenied"

    def test_s3_filter_objects_by_state(self, s3_config: SourceConfig):
        """Test filtering objects based on state."""
        source = S3Source(s3_config, {})

        objects = [
            {"Key": "data/file1.csv", "LastModified": datetime(2024, 1, 1)},
            {"Key": "data/file2.csv", "LastModified": datetime(2024, 1, 15)},
            {"Key": "data/file3.csv", "LastModified": datetime(2024, 2, 1)},
        ]

        state = State(cursor_values={"last_modified": datetime(2024, 1, 10).isoformat()})
        filtered = source._filter_objects_by_state(objects, state)

        assert len(filtered) == 2
        assert filtered[0]["Key"] == "data/file2.csv"
        assert filtered[1]["Key"] == "data/file3.csv"

    def test_s3_filter_objects_by_filename(self, s3_config: SourceConfig):
        """Test filtering objects by filename."""
        source = S3Source(s3_config, {})

        objects = [
            {"Key": "data/2024-01-01.csv", "LastModified": datetime(2024, 1, 1)},
            {"Key": "data/2024-01-15.csv", "LastModified": datetime(2024, 1, 15)},
            {"Key": "data/2024-02-01.csv", "LastModified": datetime(2024, 2, 1)},
        ]

        state = State(cursor_values={"last_file": "data/2024-01-10.csv"})
        filtered = source._filter_objects_by_state(objects, state)

        assert len(filtered) == 2
        assert filtered[0]["Key"] == "data/2024-01-15.csv"
        assert filtered[1]["Key"] == "data/2024-02-01.csv"

    def test_s3_filter_objects_empty_state(self, s3_config: SourceConfig):
        """Test that empty state returns all objects."""
        source = S3Source(s3_config, {})

        objects = [
            {"Key": "data/file1.csv", "LastModified": datetime(2024, 1, 1)},
            {"Key": "data/file2.csv", "LastModified": datetime(2024, 1, 2)},
        ]

        state = State()
        filtered = source._filter_objects_by_state(objects, state)

        assert filtered == objects

    @patch("dataloader.connectors.s3.source.boto3.client")
    def test_s3_download_error(
        self, mock_boto_client: MagicMock, s3_config: SourceConfig
    ):
        """Test that download errors raise ConnectorError."""
        from botocore.exceptions import ClientError

        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client

        mock_client.download_fileobj.side_effect = ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": "Key not found"}},
            "GetObject",
        )

        source = S3Source(s3_config, {})

        with pytest.raises(ConnectorError) as exc_info:
            source._download_object("test-bucket", "nonexistent.csv")

        assert "Failed to download S3 object" in str(exc_info.value)
        assert exc_info.value.context["error_code"] == "NoSuchKey"

    @patch("dataloader.connectors.s3.source.boto3.client")
    def test_s3_read_batches_integration(
        self, mock_boto_client: MagicMock, s3_config: SourceConfig
    ):
        """Test full read_batches flow with mocked S3."""
        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client

        # Mock list objects
        mock_paginator = MagicMock()
        mock_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {
                "Contents": [
                    {
                        "Key": "data/test.csv",
                        "LastModified": datetime(2024, 1, 1),
                        "Size": 100,
                    }
                ]
            }
        ]

        # Create temp CSV file to mock download
        csv_content = b"id,name\n1,Alice\n2,Bob\n"

        def mock_download(bucket, key, fileobj):
            fileobj.write(csv_content)

        mock_client.download_fileobj.side_effect = mock_download

        source = S3Source(s3_config, {"batch_size": 100})
        state = State()

        batches = list(source.read_batches(state))

        assert len(batches) == 1
        assert batches[0].columns == ["id", "name"]
        assert batches[0].rows == [["1", "Alice"], ["2", "Bob"]]
        assert batches[0].metadata["source_type"] == "s3"
        assert batches[0].metadata["s3_key"] == "data/test.csv"
        assert batches[0].metadata["s3_bucket"] == "test-bucket"

    def test_s3_type_inference(self, s3_config: SourceConfig):
        """Test type inference in S3 source."""
        source = S3Source(s3_config, {})

        assert source._infer_type("123") == "int"
        assert source._infer_type("45.67") == "float"
        assert source._infer_type("hello") == "string"
        assert source._infer_type("2024-01-15") == "datetime"

    def test_create_s3_source_factory(self, s3_config: SourceConfig):
        """Test the factory function creates S3Source."""
        source = create_s3_source(s3_config, {})
        assert isinstance(source, S3Source)


class TestConnectorRegistration:
    """Tests for connector registration."""

    @pytest.fixture(autouse=True)
    def ensure_registration(self):
        """Ensure built-in connectors are registered before each test."""
        from dataloader.connectors import register_builtin_connectors

        register_builtin_connectors()

    def test_sources_registered(self):
        """Test that built-in connectors are registered."""
        from dataloader.connectors import list_source_types

        types = list_source_types()
        assert "postgres" in types
        assert "csv" in types
        assert "s3" in types

    def test_get_source_returns_correct_type(self):
        """Test that get_source returns the correct connector type."""
        from dataloader.connectors import get_source

        csv_config = SourceConfig(type="csv", path="/tmp/test.csv")
        source = get_source("csv", csv_config, {})

        assert isinstance(source, CSVSource)

    def test_get_source_postgres(self):
        """Test getting PostgresSource via registry."""
        from dataloader.connectors import get_source

        config = SourceConfig(
            type="postgres",
            host="localhost",
            database="test",
            user="user",
            table="users",
        )
        source = get_source("postgres", config, {})

        assert isinstance(source, PostgresSource)

    def test_get_source_s3(self):
        """Test getting S3Source via registry."""
        from dataloader.connectors import get_source

        config = SourceConfig(
            type="s3",
            bucket="test-bucket",
            path="data/",
        )
        source = get_source("s3", config, {})

        assert isinstance(source, S3Source)

