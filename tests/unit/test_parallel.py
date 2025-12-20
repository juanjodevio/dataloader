"""Unit tests for parallel execution."""

import asyncio

import pytest

from dataloader.core.batch import Batch
from dataloader.core.exceptions import EngineError
from dataloader.core.parallel import AsyncParallelExecutor


def test_async_parallel_executor():
    """Test AsyncParallelExecutor with simple processing."""
    
    async def run_test():
        executor = AsyncParallelExecutor(concurrency=3)
        
        # Create test batches
        batches = [
            Batch(data={"col1": [1, 2, 3]}, row_count=3),
            Batch(data={"col1": [4, 5, 6]}, row_count=3),
            Batch(data={"col1": [7, 8, 9]}, row_count=3),
        ]
        
        # Process function that doubles values
        def process_batch(batch: Batch) -> Batch:
            new_data = {k: [v * 2 for v in values] for k, values in batch.data.items()}
            return Batch(data=new_data, row_count=batch.row_count)
        
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
        
        batches = [Batch(data={}, row_count=1) for _ in range(5)]
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
            Batch(data={}, row_count=1),
            Batch(data={}, row_count=2),
            Batch(data={}, row_count=3),
        ]
        
        with pytest.raises(EngineError):
            await executor.process_batches(batches, process_batch)
    
    asyncio.run(run_test())

