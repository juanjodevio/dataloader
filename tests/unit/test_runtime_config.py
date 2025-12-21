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
