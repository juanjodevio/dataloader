"""Unit tests for state backends."""

import json
import tempfile
from pathlib import Path

import pytest

from dataloader.core.exceptions import StateError
from dataloader.core.state_backend import (
    DynamoDBStateBackend,
    LocalStateBackend,
    S3StateBackend,
    create_state_backend,
)


def test_local_state_backend():
    """Test LocalStateBackend basic operations."""
    with tempfile.TemporaryDirectory() as tmpdir:
        backend = LocalStateBackend(tmpdir)

        # Save state
        state = {"cursor_values": {"id": 100}, "metadata": {"test": True}}
        backend.save("test_recipe", state)

        # Load state
        loaded = backend.load("test_recipe")
        assert loaded == state

        # Load non-existent
        empty = backend.load("nonexistent")
        assert empty == {}


def test_local_state_backend_async():
    """Test LocalStateBackend async methods."""
    import asyncio

    async def run_test():
        with tempfile.TemporaryDirectory() as tmpdir:
            backend = LocalStateBackend(tmpdir)

            state = {"cursor_values": {"id": 100}}

            # Save async
            await backend.save_async("test_recipe", state)

            # Load async
            loaded = await backend.load_async("test_recipe")
            assert loaded == state

    asyncio.run(run_test())


def test_create_state_backend_local():
    """Test state backend factory for local backend."""
    backend = create_state_backend("local:/tmp/test")
    assert isinstance(backend, LocalStateBackend)
    assert backend.state_dir == Path("/tmp/test")

    backend2 = create_state_backend("local")
    assert isinstance(backend2, LocalStateBackend)


def test_create_state_backend_s3():
    """Test state backend factory for S3 backend."""
    backend = create_state_backend("s3://my-bucket/state/")
    assert isinstance(backend, S3StateBackend)
    assert backend.bucket == "my-bucket"
    assert backend.prefix == "state/"


def test_create_state_backend_dynamodb():
    """Test state backend factory for DynamoDB backend."""
    backend = create_state_backend("dynamodb:my-table")
    assert isinstance(backend, DynamoDBStateBackend)
    assert backend.table_name == "my-table"

    backend2 = create_state_backend("dynamodb:my-table:us-east-1")
    assert isinstance(backend2, DynamoDBStateBackend)
    assert backend2.table_name == "my-table"


def test_create_state_backend_invalid():
    """Test state backend factory with invalid config."""
    with pytest.raises(ValueError):
        create_state_backend("invalid:config")
