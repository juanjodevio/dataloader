"""Unit tests for parallel execution."""

import asyncio
from typing import Any

import pytest

from dataloader.core.batch import Batch
from dataloader.core.exceptions import EngineError
from dataloader.core.parallel import AsyncParallelExecutor


class SimpleBatch:
    """Simple batch implementation for testing."""

    def __init__(self, data: dict[str, list[Any]], row_count: int):
        """Initialize test batch with data dict and row_count."""
        self.data = data
        self._row_count = row_count
        # Convert data dict to columns and rows format
        if data:
            self._columns = list(data.keys())
            # Transpose: [[col1_val1, col2_val1], [col1_val2, col2_val2], ...]
            values = list(data.values())
            if values:
                self._rows = [
                    [values[i][j] for i in range(len(values))]
                    for j in range(len(values[0]))
                ]
            else:
                self._rows = []
        else:
            self._columns = []
            self._rows = []

    @property
    def columns(self) -> list[str]:
        """Return column names."""
        return self._columns

    @property
    def rows(self) -> list[list[Any]]:
        """Return row data."""
        return self._rows

    @property
    def metadata(self) -> dict[str, Any]:
        """Return metadata."""
        return {}

    @property
    def row_count(self) -> int:
        """Return row count."""
        return self._row_count

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "columns": self._columns,
            "rows": self._rows,
            "metadata": self.metadata,
        }


def test_async_parallel_executor():
    """Test AsyncParallelExecutor with simple processing."""

    async def run_test():
        executor = AsyncParallelExecutor(concurrency=3)

        # Create test batches
        batches = [
            SimpleBatch(data={"col1": [1, 2, 3]}, row_count=3),
            SimpleBatch(data={"col1": [4, 5, 6]}, row_count=3),
            SimpleBatch(data={"col1": [7, 8, 9]}, row_count=3),
        ]

        # Process function that doubles values
        def process_batch(batch: Batch) -> Batch:
            simple_batch = batch  # type: SimpleBatch
            new_data = {
                k: [v * 2 for v in values] for k, values in simple_batch.data.items()
            }
            return SimpleBatch(data=new_data, row_count=simple_batch.row_count)

        # Process batches
        results = await executor.process_batches(batches, process_batch)

        assert len(results) == 3
        assert results[0].data["col1"] == [2, 4, 6]
        assert results[1].data["col1"] == [8, 10, 12]
        assert results[2].data["col1"] == [14, 16, 18]

    asyncio.run(run_test())


def test_async_parallel_executor_concurrency_limit():
    """Test that concurrency limit is enforced."""

    async def run_test():
        executor = AsyncParallelExecutor(concurrency=2)

        # Track concurrent executions
        concurrent_count = 0
        max_concurrent = 0
        lock = asyncio.Lock()

        async def process_batch(batch: Batch) -> Batch:
            nonlocal concurrent_count, max_concurrent
            async with lock:
                concurrent_count += 1
                max_concurrent = max(max_concurrent, concurrent_count)

            await asyncio.sleep(0.1)  # Simulate work

            async with lock:
                concurrent_count -= 1

            return batch

        batches = [SimpleBatch(data={}, row_count=1) for _ in range(5)]
        await executor.process_batches(batches, process_batch)

        # Should not exceed concurrency limit
        assert max_concurrent <= 2

    asyncio.run(run_test())


def test_async_parallel_executor_error_handling():
    """Test error handling in parallel execution."""

    async def run_test():
        executor = AsyncParallelExecutor(concurrency=2)

        def process_batch(batch: Batch) -> Batch:
            if batch.row_count == 2:
                raise ValueError("Test error")
            return batch

        batches = [
            SimpleBatch(data={}, row_count=1),
            SimpleBatch(data={}, row_count=2),
            SimpleBatch(data={}, row_count=3),
        ]

        with pytest.raises(EngineError):
            await executor.process_batches(batches, process_batch)

    asyncio.run(run_test())
