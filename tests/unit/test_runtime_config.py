"""Tests for RuntimeConfig model."""

import pytest
from pydantic import ValidationError

from dataloader.models.runtime_config import RuntimeConfig


class TestRuntimeConfig:
    """Tests for RuntimeConfig validation."""

    def test_default_values(self):
        """Test default values."""
        config = RuntimeConfig()
        assert config.batch_size == 10000
        assert config.full_refresh is False

    def test_custom_batch_size(self):
        """Test custom batch size."""
        config = RuntimeConfig(batch_size=5000)
        assert config.batch_size == 5000

    def test_invalid_batch_size_zero(self):
        """Test that batch_size must be greater than 0."""
        with pytest.raises(ValidationError) as exc_info:
            RuntimeConfig(batch_size=0)
        errors = exc_info.value.errors()
        assert any("greater than 0" in str(err) for err in errors)

    def test_invalid_batch_size_negative(self):
        """Test that batch_size cannot be negative."""
        with pytest.raises(ValidationError):
            RuntimeConfig(batch_size=-1)

    def test_default_full_refresh(self):
        """Test that full_refresh defaults to False."""
        config = RuntimeConfig()
        assert config.full_refresh is False

    def test_full_refresh_true(self):
        """Test setting full_refresh to True."""
        config = RuntimeConfig(full_refresh=True)
        assert config.full_refresh is True

    def test_full_refresh_false(self):
        """Test setting full_refresh to False."""
        config = RuntimeConfig(full_refresh=False)
        assert config.full_refresh is False

    def test_runtime_config_with_all_fields(self):
        """Test RuntimeConfig with all fields set."""
        config = RuntimeConfig(
            batch_size=5000,
            parallelism=4,
            full_refresh=True,
        )
        assert config.batch_size == 5000
        assert config.parallelism == 4
        assert config.full_refresh is True