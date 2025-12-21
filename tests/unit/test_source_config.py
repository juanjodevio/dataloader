"""Tests for SourceConfig model."""

import pytest
from pydantic import ValidationError

from dataloader.models.source_config import IncrementalConfig, SourceConfig


class TestIncrementalConfig:
    """Tests for IncrementalConfig."""

    def test_valid_cursor_config(self):
        """Test valid cursor incremental config."""
        config = IncrementalConfig(strategy="cursor", cursor_column="updated_at")
        assert config.strategy == "cursor"
        assert config.cursor_column == "updated_at"

    def test_invalid_strategy(self):
        """Test that only cursor strategy is allowed."""
        with pytest.raises(ValidationError):
            IncrementalConfig(strategy="watermark", cursor_column="updated_at")


class TestSourceConfig:
    """Tests for SourceConfig validation."""

    def test_postgres_source_valid(self):
        """Test valid Postgres source config."""
        config = SourceConfig(
            type="postgres",
            host="localhost",
            database="testdb",
            user="testuser",
            password="testpass",
            table="public.users",
        )
        assert config.type == "postgres"
        assert config.host == "localhost"
        assert config.table == "public.users"

    def test_postgres_source_missing_required_fields(self):
        """Test Postgres source with missing required fields."""
        with pytest.raises(ValidationError) as exc_info:
            SourceConfig(
                type="postgres",
                host="localhost",
                # missing database, user, table
            )
        errors = exc_info.value.errors()
        assert any("requires fields" in str(err) for err in errors)

    def test_filestore_source_valid_local(self):
        """Test valid FileStore source config with local backend."""
        config = SourceConfig(
            type="filestore",
            backend="local",
            filepath="/path/to/file.csv",
            format="csv",
        )
        assert config.type == "filestore"
        assert config.backend == "local"
        assert config.filepath == "/path/to/file.csv"
        assert config.format == "csv"

    def test_filestore_source_valid_s3(self):
        """Test valid FileStore source config with S3 backend."""
        config = SourceConfig(
            type="filestore",
            backend="s3",
            filepath="s3://my-bucket/data/file.csv",
            format="csv",
        )
        assert config.type == "filestore"
        assert config.backend == "s3"
        assert config.filepath == "s3://my-bucket/data/file.csv"
        assert config.format == "csv"

    def test_filestore_source_s3_inferred_from_path(self):
        """Test FileStore source with S3 backend inferred from s3:// path."""
        config = SourceConfig(
            type="filestore",
            filepath="s3://my-bucket/data/file.csv",
            format="csv",
        )
        assert config.type == "filestore"
        assert config.filepath == "s3://my-bucket/data/file.csv"
        assert config.format == "csv"

    def test_filestore_source_missing_filepath(self):
        """Test FileStore source with missing filepath."""
        with pytest.raises(ValidationError) as exc_info:
            SourceConfig(
                type="filestore",
                backend="local",
                format="csv",
                # missing filepath
            )
        errors = exc_info.value.errors()
        assert any("requires 'filepath' field" in str(err) for err in errors)

    def test_filestore_source_missing_format(self):
        """Test FileStore source with missing format."""
        with pytest.raises(ValidationError) as exc_info:
            SourceConfig(
                type="filestore",
                backend="local",
                filepath="/path/to/file.csv",
                # missing format
            )
        errors = exc_info.value.errors()
        assert any("requires 'format' field" in str(err) for err in errors)

    def test_duckdb_source_valid(self):
        """Test valid DuckDB source config."""
        config = SourceConfig(
            type="duckdb",
            database=":memory:",
            table="users",
        )
        assert config.type == "duckdb"
        assert config.database == ":memory:"
        assert config.table == "users"

    def test_duckdb_source_missing_table(self):
        """Test DuckDB source with missing table."""
        with pytest.raises(ValidationError) as exc_info:
            SourceConfig(
                type="duckdb",
                database=":memory:",
                # missing table
            )
        errors = exc_info.value.errors()
        assert any("requires 'table' field" in str(err) for err in errors)

    def test_incremental_config(self):
        """Test source with incremental config."""
        config = SourceConfig(
            type="postgres",
            host="localhost",
            database="testdb",
            user="testuser",
            password="testpass",
            table="public.users",
            incremental=IncrementalConfig(strategy="cursor", cursor_column="updated_at"),
        )
        assert config.incremental is not None
        assert config.incremental.strategy == "cursor"
        assert config.incremental.cursor_column == "updated_at"

    def test_optional_fields(self):
        """Test that optional fields can be None."""
        config = SourceConfig(
            type="postgres",
            host="localhost",
            database="testdb",
            user="testuser",
            password="testpass",
            table="public.users",
            port=None,
            db_schema=None,
        )
        assert config.port is None
        assert config.db_schema is None

