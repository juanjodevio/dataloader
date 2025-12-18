"""Pytest configuration and shared fixtures."""

import os
import tempfile
from pathlib import Path

import pytest


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def recipe_dir(temp_dir):
    """Create a recipes subdirectory in temp_dir."""
    recipes_dir = temp_dir / "recipes"
    recipes_dir.mkdir()
    return recipes_dir


@pytest.fixture
def env_vars(monkeypatch):
    """Fixture to set environment variables for testing."""
    test_vars = {
        "TEST_HOST": "test.example.com",
        "TEST_USER": "testuser",
        "TEST_PASSWORD": "testpass",
        "TEST_DB": "testdb",
    }
    for key, value in test_vars.items():
        monkeypatch.setenv(key, value)
    return test_vars


@pytest.fixture
def cli_vars():
    """Fixture providing CLI variables for testing."""
    return {
        "CLI_VAR_1": "cli_value_1",
        "CLI_VAR_2": "cli_value_2",
    }

