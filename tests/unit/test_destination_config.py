"""Tests for DestinationConfig model."""

import pytest
from pydantic import ValidationError

from dataloader.models.destination_config import DestinationConfig


class TestDestinationConfig:
    """Tests for DestinationConfig validation."""

    def test_redshift_destination_valid(self):
        """Test valid Redshift destination config."""
        config = DestinationConfig(
            type="redshift",
            host="redshift.example.com",
            database="dw",
            user="dwuser",
            password="dwpass",
            table="dw.customers",
            write_mode="append",
        )
        assert config.type == "redshift"
        assert config.host == "redshift.example.com"
        assert config.table == "dw.customers"
        assert config.write_mode == "append"

    def test_redshift_destination_missing_required_fields(self):
        """Test Redshift destination with missing required fields."""
        with pytest.raises(ValidationError) as exc_info:
            DestinationConfig(
                type="redshift",
                host="redshift.example.com",
                # missing database, user, table
            )
        errors = exc_info.value.errors()
        assert any("requires fields" in str(err) for err in errors)

    def test_merge_mode_requires_merge_keys(self):
        """Test that merge mode requires merge_keys."""
        with pytest.raises(ValidationError) as exc_info:
            DestinationConfig(
                type="redshift",
                host="redshift.example.com",
                database="dw",
                user="dwuser",
                password="dwpass",
                table="dw.customers",
                write_mode="merge",
                # missing merge_keys
            )
        errors = exc_info.value.errors()
        assert any("merge_keys is required" in str(err) for err in errors)

    def test_merge_mode_with_merge_keys(self):
        """Test merge mode with merge_keys provided."""
        config = DestinationConfig(
            type="redshift",
            host="redshift.example.com",
            database="dw",
            user="dwuser",
            password="dwpass",
            table="dw.customers",
            write_mode="merge",
            merge_keys=["id"],
        )
        assert config.write_mode == "merge"
        assert config.merge_keys == ["id"]

    def test_filestore_destination_valid(self):
        """Test valid FileStore destination config."""
        config = DestinationConfig(
            type="filestore",
            filepath="s3://my-bucket/output/data.parquet",
            format="parquet",
            write_mode="overwrite",
        )
        assert config.type == "filestore"
        assert config.filepath == "s3://my-bucket/output/data.parquet"
        assert config.format == "parquet"
        assert config.write_mode == "overwrite"

    def test_filestore_destination_missing_filepath(self):
        """Test FileStore destination with missing filepath."""
        with pytest.raises(ValidationError) as exc_info:
            DestinationConfig(
                type="filestore",
                format="parquet",
                # missing filepath
            )
        errors = exc_info.value.errors()
        assert any("requires 'filepath' field" in str(err) for err in errors)

    def test_write_modes(self):
        """Test all write modes."""
        for mode in ["append", "overwrite", "merge"]:
            config = DestinationConfig(
                type="redshift",
                host="localhost",
                database="testdb",
                user="testuser",
                password="testpass",
                table="test.table",
                write_mode=mode,
                merge_keys=["id"] if mode == "merge" else None,
            )
            assert config.write_mode == mode

