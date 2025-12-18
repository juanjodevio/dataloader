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

    def test_s3_source_valid(self):
        """Test valid S3 source config."""
        config = SourceConfig(
            type="s3",
            bucket="my-bucket",
            path="data/file.csv",
        )
        assert config.type == "s3"
        assert config.bucket == "my-bucket"
        assert config.path == "data/file.csv"

    def test_s3_source_missing_bucket(self):
        """Test S3 source with missing bucket."""
        with pytest.raises(ValidationError) as exc_info:
            SourceConfig(
                type="s3",
                path="data/file.csv",
                # missing bucket
            )
        errors = exc_info.value.errors()
        assert any("requires 'bucket' and 'path'" in str(err) for err in errors)

    def test_csv_source_valid(self):
        """Test valid CSV source config."""
        config = SourceConfig(
            type="csv",
            path="/path/to/file.csv",
        )
        assert config.type == "csv"
        assert config.path == "/path/to/file.csv"

    def test_csv_source_missing_path(self):
        """Test CSV source with missing path."""
        with pytest.raises(ValidationError) as exc_info:
            SourceConfig(
                type="csv",
                # missing path
            )
        errors = exc_info.value.errors()
        assert any("requires 'path'" in str(err) for err in errors)

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

