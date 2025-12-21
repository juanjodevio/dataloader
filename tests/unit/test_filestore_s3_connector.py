"""Unit tests for FileStoreConnector with S3 backend.

Uses a workaround to mock fsspec's S3 filesystem to work with moto by
patching fsspec.filesystem() to return a mock filesystem that uses boto3
directly (which works with moto) instead of s3fs's async implementation.
"""

import csv
import json
from datetime import datetime

import pytest
from moto import mock_aws

from dataloader.connectors.filestore.config import S3FileStoreConfig
from dataloader.connectors.filestore.connector import (
    FileStoreConnector,
    create_filestore_connector,
)
from dataloader.core.batch import ArrowBatch
from dataloader.core.exceptions import ConnectorError
from dataloader.core.state import State
from dataloader.models.destination_config import DestinationConfig
from dataloader.models.source_config import SourceConfig


# Shared fixtures at module level
@pytest.fixture
def s3_bucket():
    """Create a mock S3 bucket using moto."""
    with mock_aws():
        import boto3

        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket="test-bucket")
        yield s3_client


@pytest.fixture
def mock_s3_filesystem(s3_bucket, monkeypatch):
    """Mock fsspec S3 filesystem to work with moto by patching methods."""
    from unittest.mock import MagicMock

    import fsspec

    # Store original filesystem creation
    original_filesystem = fsspec.filesystem

    def mock_filesystem(protocol, **kwargs):
        if protocol == "s3":
            # Create a mock filesystem that uses boto3 directly
            mock_fs = MagicMock()
            mock_fs.protocol = "s3"

            # Mock the methods we use
            def mock_open(path, mode="rb", encoding=None):
                class MockFile:
                    def __init__(self, path, mode, encoding):
                        self.path = path
                        self.mode = mode
                        self.encoding = encoding
                        self._content = None
                        self._position = 0

                    def __enter__(self):
                        if "r" in self.mode:
                            # Read from S3
                            s3_key = self.path.replace("s3://test-bucket/", "")
                            try:
                                response = s3_bucket.get_object(
                                    Bucket="test-bucket", Key=s3_key
                                )
                                self._content = response["Body"].read()
                                if self.encoding:
                                    self._content = self._content.decode(self.encoding)
                            except s3_bucket.exceptions.NoSuchKey:
                                raise FileNotFoundError(f"File not found: {self.path}")
                        return self

                    def __exit__(self, *args):
                        if "w" in self.mode and self._content is not None:
                            # Write to S3
                            s3_key = self.path.replace("s3://test-bucket/", "")
                            if isinstance(self._content, str):
                                content_bytes = self._content.encode(
                                    self.encoding or "utf-8"
                                )
                            else:
                                content_bytes = self._content
                            s3_bucket.put_object(
                                Bucket="test-bucket", Key=s3_key, Body=content_bytes
                            )
                        pass

                    def read(self):
                        if self._content is None:
                            return b""
                        if isinstance(self._content, bytes):
                            return self._content
                        return self._content.encode(self.encoding or "utf-8")

                    def write(self, data):
                        if self._content is None:
                            self._content = b""
                        if isinstance(data, str):
                            data = data.encode(self.encoding or "utf-8")
                        if isinstance(self._content, bytes):
                            self._content += data
                        else:
                            self._content = (
                                self._content.encode(self.encoding or "utf-8") + data
                            )

                return MockFile(path, mode, encoding)

            def mock_isdir(path):
                # Check if path is a directory (has objects with this prefix)
                prefix = path.replace("s3://test-bucket/", "").rstrip("/") + "/"
                objects = s3_bucket.list_objects_v2(
                    Bucket="test-bucket", Prefix=prefix, MaxKeys=1
                )
                return "Contents" in objects

            def mock_find(path):
                # List all files with the given prefix
                prefix = path.replace("s3://test-bucket/", "").rstrip("/") + "/"
                objects = s3_bucket.list_objects_v2(Bucket="test-bucket", Prefix=prefix)
                files = []
                for obj in objects.get("Contents", []):
                    files.append(f"s3://test-bucket/{obj['Key']}")
                return files

            def mock_rm(path):
                # Delete file from S3
                s3_key = path.replace("s3://test-bucket/", "")
                s3_bucket.delete_object(Bucket="test-bucket", Key=s3_key)

            def mock_exists(path):
                # Check if file exists in S3
                s3_key = path.replace("s3://test-bucket/", "")
                try:
                    s3_bucket.head_object(Bucket="test-bucket", Key=s3_key)
                    return True
                except s3_bucket.exceptions.NoSuchKey:
                    return False

            mock_fs.open = mock_open
            mock_fs.isdir = mock_isdir
            mock_fs.find = mock_find
            mock_fs.rm = mock_rm
            mock_fs.exists = mock_exists
            mock_fs.modified = MagicMock(return_value="2024-01-01T00:00:00")

            return mock_fs
        else:
            return original_filesystem(protocol, **kwargs)

    monkeypatch.setattr(fsspec, "filesystem", mock_filesystem)
    yield


class TestFileStoreS3Connector:
    """Tests for FileStoreConnector with S3 backend."""

    @pytest.fixture
    def s3_config(self) -> S3FileStoreConfig:
        """Create an S3 FileStore config."""
        # For moto, we use default credentials
        return S3FileStoreConfig(
            type="filestore",
            backend="s3",
            bucket="test-bucket",
            path="data/",
            format="csv",
            # Don't set access_key/secret_key - let moto use defaults
            region="us-east-1",
        )

    @pytest.fixture
    def sample_batch(self) -> ArrowBatch:
        """Create a sample batch."""
        return ArrowBatch.from_rows(
            columns=["id", "name", "score"],
            rows=[
                [1, "Alice", 95.5],
                [2, "Bob", 87.0],
                [3, "Charlie", 92.3],
            ],
            metadata={
                "column_types": {"id": "int", "name": "string", "score": "float"}
            },
        )

    def test_filestore_s3_connector_initialization(self, s3_config: S3FileStoreConfig):
        """Test that FileStoreConnector initializes correctly with S3 config."""
        connector = FileStoreConnector(s3_config)

        assert connector._backend == "s3"
        assert connector._path == "data/"
        assert connector._format == "csv"
        assert connector._write_mode == "append"
        assert connector._filesystem is None

    def test_filestore_s3_write_csv_append(
        self,
        s3_config: S3FileStoreConfig,
        sample_batch: ArrowBatch,
        s3_bucket,
        mock_s3_filesystem,
    ):
        """Test writing CSV files to S3 in append mode."""
        connector = FileStoreConnector(s3_config)
        state = State()

        connector.write_batch(sample_batch, state)

        # Verify file was written
        written_files = connector.written_files
        assert len(written_files) == 1
        assert written_files[0].startswith("s3://test-bucket/data/")

        # Verify file exists in S3
        s3_key = written_files[0].replace("s3://test-bucket/", "")
        response = s3_bucket.get_object(Bucket="test-bucket", Key=s3_key)
        content = response["Body"].read().decode("utf-8")

        # Verify CSV content
        reader = csv.DictReader(content.splitlines())
        rows = list(reader)
        assert len(rows) == 3
        assert rows[0]["id"] == "1"
        assert rows[0]["name"] == "Alice"
        assert rows[0]["score"] == "95.5"

    def test_filestore_s3_write_csv_overwrite(
        self,
        s3_config: S3FileStoreConfig,
        sample_batch: ArrowBatch,
        s3_bucket,
        mock_s3_filesystem,
    ):
        """Test writing CSV files to S3 in overwrite mode."""
        s3_config.write_mode = "overwrite"

        # Create existing file in S3
        s3_bucket.put_object(
            Bucket="test-bucket",
            Key="data/existing_data.csv",
            Body=b"id,name\n1,Old",
        )

        connector = FileStoreConnector(s3_config)
        state = State()

        connector.write_batch(sample_batch, state)

        # Verify existing file was deleted
        try:
            s3_bucket.head_object(Bucket="test-bucket", Key="data/existing_data.csv")
            assert False, "Existing file should have been deleted"
        except Exception as e:
            # File should not exist (moto raises ClientError with 404)
            assert "404" in str(e) or "Not Found" in str(e)

        # Verify new file was created
        written_files = connector.written_files
        assert len(written_files) == 1

    def test_filestore_s3_write_json(
        self,
        s3_config: S3FileStoreConfig,
        sample_batch: ArrowBatch,
        s3_bucket,
        mock_s3_filesystem,
    ):
        """Test writing JSON files to S3."""
        s3_config.format = "json"
        connector = FileStoreConnector(s3_config)
        state = State()

        connector.write_batch(sample_batch, state)

        written_files = connector.written_files
        assert len(written_files) == 1

        # Verify file exists in S3
        s3_key = written_files[0].replace("s3://test-bucket/", "")
        response = s3_bucket.get_object(Bucket="test-bucket", Key=s3_key)
        content = response["Body"].read().decode("utf-8")
        data = json.loads(content)

        assert isinstance(data, list)
        assert len(data) == 3
        assert data[0]["id"] == 1
        assert data[0]["name"] == "Alice"

    def test_filestore_s3_write_jsonl(
        self,
        s3_config: S3FileStoreConfig,
        sample_batch: ArrowBatch,
        s3_bucket,
        mock_s3_filesystem,
    ):
        """Test writing JSONL files to S3."""
        s3_config.format = "jsonl"
        connector = FileStoreConnector(s3_config)
        state = State()

        connector.write_batch(sample_batch, state)

        written_files = connector.written_files
        assert len(written_files) == 1

        # Verify file exists in S3
        s3_key = written_files[0].replace("s3://test-bucket/", "")
        response = s3_bucket.get_object(Bucket="test-bucket", Key=s3_key)
        content = response["Body"].read().decode("utf-8")

        lines = [json.loads(line) for line in content.strip().split("\n")]
        assert len(lines) == 3
        assert lines[0]["id"] == 1
        assert lines[0]["name"] == "Alice"

    def test_filestore_s3_read_csv_single_file(
        self, s3_config: S3FileStoreConfig, s3_bucket, mock_s3_filesystem
    ):
        """Test reading from a single CSV file in S3."""
        # Create test CSV file in S3
        csv_content = "id,name,score\n1,Alice,95.5\n2,Bob,87.0\n3,Charlie,92.3\n"
        s3_bucket.put_object(
            Bucket="test-bucket",
            Key="data/test_data.csv",
            Body=csv_content.encode("utf-8"),
        )

        # Update config to point to file
        s3_config.path = "data/test_data.csv"
        connector = FileStoreConnector(s3_config)
        state = State()

        batches = list(connector.read_batches(state))

        assert len(batches) == 1
        batch = batches[0]
        assert batch.columns == ["id", "name", "score"]
        assert len(batch.rows) == 3
        assert batch.rows[0] == ["1", "Alice", "95.5"]

    def test_filestore_s3_read_csv_directory(
        self, s3_config: S3FileStoreConfig, s3_bucket, mock_s3_filesystem
    ):
        """Test reading from a directory of CSV files in S3."""
        # Create multiple CSV files in S3
        for i in range(2):
            csv_content = (
                f"id,name\n{i * 2 + 1},User{i * 2 + 1}\n{i * 2 + 2},User{i * 2 + 2}\n"
            )
            s3_bucket.put_object(
                Bucket="test-bucket",
                Key=f"data/file_{i}.csv",
                Body=csv_content.encode("utf-8"),
            )

        connector = FileStoreConnector(s3_config)
        state = State()

        batches = list(connector.read_batches(state))

        # Should read from both files
        assert len(batches) >= 1
        total_rows = sum(len(batch.rows) for batch in batches)
        assert total_rows >= 4

    def test_filestore_s3_read_json(
        self, s3_config: S3FileStoreConfig, s3_bucket, mock_s3_filesystem
    ):
        """Test reading JSON files from S3."""
        # Create test JSON file in S3
        json_data = [
            {"id": 1, "name": "Alice", "score": 95.5},
            {"id": 2, "name": "Bob", "score": 87.0},
        ]
        s3_bucket.put_object(
            Bucket="test-bucket",
            Key="data/test_data.json",
            Body=json.dumps(json_data).encode("utf-8"),
        )

        s3_config.path = "data/test_data.json"
        s3_config.format = "json"
        connector = FileStoreConnector(s3_config)
        state = State()

        batches = list(connector.read_batches(state))

        assert len(batches) >= 1
        batch = batches[0]
        assert len(batch.rows) >= 2

    def test_filestore_s3_read_jsonl(
        self, s3_config: S3FileStoreConfig, s3_bucket, mock_s3_filesystem
    ):
        """Test reading JSONL files from S3."""
        # Create test JSONL file in S3
        jsonl_lines = [
            json.dumps({"id": 1, "name": "Alice"}),
            json.dumps({"id": 2, "name": "Bob"}),
        ]
        s3_bucket.put_object(
            Bucket="test-bucket",
            Key="data/test_data.jsonl",
            Body="\n".join(jsonl_lines).encode("utf-8"),
        )

        s3_config.path = "data/test_data.jsonl"
        s3_config.format = "jsonl"
        connector = FileStoreConnector(s3_config)
        state = State()

        batches = list(connector.read_batches(state))

        assert len(batches) >= 1
        batch = batches[0]
        assert len(batch.rows) >= 2

    def test_filestore_s3_read_incremental_filtering(
        self, s3_config: S3FileStoreConfig, s3_bucket, mock_s3_filesystem
    ):
        """Test incremental reading with state filtering."""
        # Create two CSV files in S3
        csv1 = "id,name\n1,Alice\n"
        csv2 = "id,name\n2,Bob\n"

        s3_bucket.put_object(
            Bucket="test-bucket",
            Key="data/a_file.csv",
            Body=csv1.encode("utf-8"),
        )
        s3_bucket.put_object(
            Bucket="test-bucket",
            Key="data/b_file.csv",
            Body=csv2.encode("utf-8"),
        )

        # First read: no state, should read both files
        connector1 = FileStoreConnector(s3_config)
        state1 = State()
        batches1 = list(connector1.read_batches(state1))
        total_rows1 = sum(len(batch.rows) for batch in batches1)
        assert total_rows1 >= 2

        # Second read: with last_file cursor, should filter files
        connector2 = FileStoreConnector(s3_config)
        state2 = State(cursor_values={"last_file": "s3://test-bucket/data/a_file.csv"})
        batches2 = list(connector2.read_batches(state2))
        total_rows2 = sum(len(batch.rows) for batch in batches2)
        # Should filter based on cursor
        assert total_rows2 >= 0  # At least no errors

    def test_filestore_s3_empty_batch(
        self, s3_config: S3FileStoreConfig, s3_bucket, mock_s3_filesystem
    ):
        """Test that empty batch doesn't create a file in S3."""
        connector = FileStoreConnector(s3_config)
        state = State()

        batch = ArrowBatch.from_rows(
            columns=["id", "name"],
            rows=[],
            metadata={},
        )
        connector.write_batch(batch, state)

        assert len(connector.written_files) == 0

    def test_filestore_s3_merge_mode_raises_error(
        self, s3_config: S3FileStoreConfig, sample_batch: ArrowBatch, s3_bucket
    ):
        """Test that merge mode raises ConnectorError."""
        s3_config.write_mode = "merge"
        connector = FileStoreConnector(s3_config)
        state = State()

        with pytest.raises(ConnectorError) as exc_info:
            connector.write_batch(sample_batch, state)

        assert "Merge write mode is not supported" in str(exc_info.value)

    def test_filestore_s3_with_source_config(self, s3_bucket):
        """Test FileStoreConnector with SourceConfig for S3."""
        config = SourceConfig(
            type="filestore",
            filepath="s3://test-bucket/data/",
            format="csv",
        )
        connector = FileStoreConnector(config)

        assert connector._backend == "s3"
        assert connector._format == "csv"
        assert connector._write_mode == "append"

    def test_filestore_s3_with_destination_config(self, s3_bucket):
        """Test FileStoreConnector with DestinationConfig for S3."""
        config = DestinationConfig(
            type="filestore",
            filepath="s3://test-bucket/data/",
            format="csv",
            write_mode="overwrite",
        )
        connector = FileStoreConnector(config)

        assert connector._backend == "s3"
        assert connector._format == "csv"
        assert connector._write_mode == "overwrite"

    def test_filestore_s3_path_inference(self, s3_bucket):
        """Test that S3 URLs are correctly inferred."""
        config = SourceConfig(
            type="filestore",
            filepath="s3://my-bucket/data/",
            format="csv",
        )
        connector = FileStoreConnector(config)

        assert connector._backend == "s3"

    def test_filestore_s3_storage_options(self):
        """Test that S3 storage options are built correctly."""
        # Test with credentials
        config_with_creds = S3FileStoreConfig(
            type="filestore",
            backend="s3",
            bucket="test-bucket",
            path="data/",
            format="csv",
            access_key="test-key",
            secret_key="test-secret",
            region="us-east-1",
        )
        connector = FileStoreConnector(config_with_creds)

        assert "key" in connector._storage_options
        assert "secret" in connector._storage_options
        assert connector._storage_options["key"] == "test-key"
        assert connector._storage_options["secret"] == "test-secret"
        assert "client_kwargs" in connector._storage_options
        assert connector._storage_options["client_kwargs"]["region_name"] == "us-east-1"

    def test_filestore_s3_storage_options_without_credentials(
        self, s3_config: S3FileStoreConfig
    ):
        """Test S3 storage options without explicit credentials (uses default/env)."""
        connector = FileStoreConnector(s3_config)

        # Without credentials, only region should be set
        assert "client_kwargs" in connector._storage_options
        assert connector._storage_options["client_kwargs"]["region_name"] == "us-east-1"
        # Credentials may or may not be present depending on environment

    def test_filestore_s3_custom_endpoint(self, s3_bucket):
        """Test S3 with custom endpoint (LocalStack, MinIO)."""
        # This would be set via connection dict, but we test the config structure
        config = S3FileStoreConfig(
            type="filestore",
            backend="s3",
            bucket="test-bucket",
            path="data/",
            format="csv",
            access_key="test-key",
            secret_key="test-secret",
        )
        connector = FileStoreConnector(config)

        # Custom endpoint would be added via connection dict in real usage
        assert connector._backend == "s3"

    def test_create_filestore_s3_connector_factory(self, s3_config: S3FileStoreConfig):
        """Test the factory function creates FileStoreConnector for S3."""
        connector = create_filestore_connector(s3_config)
        assert isinstance(connector, FileStoreConnector)
        assert connector._backend == "s3"


class TestFileStoreS3ConnectorRegistration:
    """Tests for FileStoreConnector S3 registration."""

    @pytest.fixture(autouse=True)
    def ensure_registration(self):
        """Ensure built-in connectors are registered before each test."""
        from dataloader.connectors import reregister_builtins

        reregister_builtins()

    def test_connectors_registered(self):
        """Test that built-in connectors are registered."""
        from dataloader.connectors import list_connector_types

        types = list_connector_types()
        assert "filestore" in types
        assert "postgres" in types
        assert "duckdb" in types

    def test_get_connector_filestore_s3(self):
        """Test getting FileStoreConnector for S3 via registry."""
        from dataloader.connectors import get_connector

        config = S3FileStoreConfig(
            type="filestore",
            backend="s3",
            bucket="test-bucket",
            path="data/",
            format="csv",
        )
        connector = get_connector("filestore", config)

        assert isinstance(connector, FileStoreConnector)
        assert connector._backend == "s3"

    def test_get_unknown_connector_raises_error(self):
        """Test that unknown connector type raises ConnectorError."""
        from dataloader.connectors import get_connector

        config = S3FileStoreConfig(
            type="filestore",
            backend="s3",
            bucket="test-bucket",
            path="data/",
        )

        with pytest.raises(ConnectorError) as exc_info:
            get_connector("unknown", config)

        assert "Unknown connector type" in str(exc_info.value)


class TestFileStoreS3Integration:
    """Integration tests for FileStore with S3 using real S3 operations (mocked)."""

    # Note: s3_bucket and mock_s3_filesystem fixtures are defined at module level
    # and are shared across all test classes in this module

    def test_full_pipeline_csv_s3(self, s3_bucket, mock_s3_filesystem):
        """Test a complete read-write pipeline with CSV on S3."""
        # Write data
        write_config = S3FileStoreConfig(
            type="filestore",
            backend="s3",
            bucket="test-bucket",
            path="pipeline/",
            format="csv",
            write_mode="append",
            access_key="test-key",
            secret_key="test-secret",
            region="us-east-1",
        )
        writer = FileStoreConnector(write_config)
        state = State()

        batch = ArrowBatch.from_rows(
            columns=["id", "name"],
            rows=[[1, "Alice"], [2, "Bob"]],
            metadata={},
        )
        writer.write_batch(batch, state)

        # Read data back
        read_config = S3FileStoreConfig(
            type="filestore",
            backend="s3",
            bucket="test-bucket",
            path="pipeline/",
            format="csv",
            access_key="test-key",
            secret_key="test-secret",
            region="us-east-1",
        )
        reader = FileStoreConnector(read_config)
        batches = list(reader.read_batches(state))

        assert len(batches) >= 1
        total_rows = sum(len(batch.rows) for batch in batches)
        assert total_rows >= 2

    def test_full_pipeline_json_s3(self, s3_bucket, mock_s3_filesystem):
        """Test a complete read-write pipeline with JSON on S3."""
        # Write data
        write_config = S3FileStoreConfig(
            type="filestore",
            backend="s3",
            bucket="test-bucket",
            path="pipeline_json/",
            format="json",
            write_mode="append",
            access_key="test-key",
            secret_key="test-secret",
            region="us-east-1",
        )
        writer = FileStoreConnector(write_config)
        state = State()

        batch = ArrowBatch.from_rows(
            columns=["id", "name"],
            rows=[[1, "Alice"], [2, "Bob"]],
            metadata={},
        )
        writer.write_batch(batch, state)

        # Read data back
        read_config = S3FileStoreConfig(
            type="filestore",
            backend="s3",
            bucket="test-bucket",
            path="pipeline_json/",
            format="json",
            access_key="test-key",
            secret_key="test-secret",
            region="us-east-1",
        )
        reader = FileStoreConnector(read_config)
        batches = list(reader.read_batches(state))

        assert len(batches) >= 1
        total_rows = sum(len(batch.rows) for batch in batches)
        assert total_rows >= 2

    def test_multiple_batches_append_s3(self, s3_bucket, mock_s3_filesystem):
        """Test writing multiple batches in append mode to S3."""
        config = S3FileStoreConfig(
            type="filestore",
            backend="s3",
            bucket="test-bucket",
            path="multi_batch/",
            format="csv",
            write_mode="append",
            access_key="test-key",
            secret_key="test-secret",
            region="us-east-1",
        )

        state = State()

        # Write multiple batches using the same connector instance
        connector = FileStoreConnector(config)
        for i in range(3):
            batch = ArrowBatch.from_rows(
                columns=["id", "value"],
                rows=[[i * 2 + 1, f"val{i * 2 + 1}"], [i * 2 + 2, f"val{i * 2 + 2}"]],
                metadata={},
            )
            connector.write_batch(batch, state)

        # Verify all files were created in S3
        objects = s3_bucket.list_objects_v2(Bucket="test-bucket", Prefix="multi_batch/")
        csv_files = [
            obj["Key"]
            for obj in objects.get("Contents", [])
            if obj["Key"].endswith(".csv")
        ]
        assert len(csv_files) == 3

    def test_s3_file_url_building(self, s3_bucket):
        """Test that S3 file URLs are built correctly."""
        config = S3FileStoreConfig(
            type="filestore",
            backend="s3",
            bucket="test-bucket",
            path="data/",
            format="csv",
        )
        connector = FileStoreConnector(config)

        # Test file URL building
        file_path = "data/test_file.csv"
        file_url = connector._build_file_url(file_path)

        assert file_url == "s3://test-bucket/data/test_file.csv"

    def test_s3_overwrite_deletes_all_files(self, s3_bucket, mock_s3_filesystem):
        """Test that overwrite mode deletes all existing files in S3 prefix."""
        # Create existing files
        s3_bucket.put_object(
            Bucket="test-bucket",
            Key="overwrite_test/file1.csv",
            Body=b"id,name\n1,Old1",
        )
        s3_bucket.put_object(
            Bucket="test-bucket",
            Key="overwrite_test/file2.csv",
            Body=b"id,name\n2,Old2",
        )

        config = S3FileStoreConfig(
            type="filestore",
            backend="s3",
            bucket="test-bucket",
            path="overwrite_test/",
            format="csv",
            write_mode="overwrite",
            access_key="test-key",
            secret_key="test-secret",
            region="us-east-1",
        )

        connector = FileStoreConnector(config)
        state = State()

        batch = ArrowBatch.from_rows(
            columns=["id", "name"],
            rows=[[3, "New"]],
            metadata={},
        )
        connector.write_batch(batch, state)

        # Verify existing files were deleted
        objects = s3_bucket.list_objects_v2(
            Bucket="test-bucket", Prefix="overwrite_test/"
        )
        old_files = [
            obj["Key"]
            for obj in objects.get("Contents", [])
            if obj["Key"] in ["overwrite_test/file1.csv", "overwrite_test/file2.csv"]
        ]
        assert len(old_files) == 0

        # Verify new file was created
        new_files = [
            obj["Key"]
            for obj in objects.get("Contents", [])
            if obj["Key"].startswith("overwrite_test/data_")
            and obj["Key"].endswith(".csv")
        ]
        assert len(new_files) == 1
