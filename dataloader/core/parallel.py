"""Async parallel execution for batch processing."""

import asyncio
from typing import Any, Callable, Iterable, TypeVar

from dataloader.core.batch import Batch
from dataloader.core.exceptions import EngineError

T = TypeVar("T")
R = TypeVar("R")


class AsyncParallelExecutor:
    """Async executor for parallel batch processing using asyncio.
    
    Processes batches concurrently while maintaining order. Uses asyncio.Semaphore
    to limit concurrency and asyncio.gather() to collect results.
    """

    def __init__(self, concurrency: int):
        """Initialize async parallel executor.
        
        Args:
            concurrency: Maximum number of concurrent batch processing tasks
        """
        if concurrency < 1:
            raise ValueError("concurrency must be at least 1")
        self.concurrency = concurrency
        self.semaphore = asyncio.Semaphore(concurrency)

    async def process_batches(
        self,
        batches: Iterable[Batch],
        process_func: Callable[[Batch], R],
    ) -> list[R]:
        """Process batches in parallel while maintaining order.
        
        Args:
            batches: Iterable of batches to process
            process_func: Function to process each batch (can be sync or async)
        
        Returns:
            List of results in the same order as input batches
        
        Raises:
            EngineError: If processing fails
        """
        # Convert batches to list to preserve order
        batch_list = list(batches)
        
        if not batch_list:
            return []
        
        # Create tasks for each batch
        tasks = [
            self._process_with_semaphore(batch, process_func, index)
            for index, batch in enumerate(batch_list)
        ]
        
        # Wait for all tasks to complete
        try:
            results = await asyncio.gather(*tasks, return_exceptions=True)
        except Exception as e:
            raise EngineError(
                f"Parallel execution failed: {e}",
                context={"concurrency": self.concurrency, "batch_count": len(batch_list)},
            ) from e
        
        # Check for exceptions in results
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                raise EngineError(
                    f"Batch {i} processing failed: {result}",
                    context={"batch_index": i, "concurrency": self.concurrency},
                ) from result
            processed_results.append(result)
        
        return processed_results

    async def _process_with_semaphore(
        self,
        batch: Batch,
        process_func: Callable[[Batch], R],
        index: int,
    ) -> R:
        """Process a single batch with semaphore control.
        
        Args:
            batch: Batch to process
            process_func: Function to process the batch
            index: Batch index for ordering
        
        Returns:
            Result from process_func
        """
        async with self.semaphore:
            # Check if process_func is async
            if asyncio.iscoroutinefunction(process_func):
                return await process_func(batch)
            else:
                # Run sync function in thread pool
                loop = asyncio.get_event_loop()
                return await loop.run_in_executor(None, process_func, batch)


def run_async(coro):
    """Run async coroutine in sync context.
    
    Creates event loop if needed and runs the coroutine.
    
    Args:
        coro: Coroutine to run
    
    Returns:
        Result of coroutine
    """
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # If loop is already running, create a new one
            loop = asyncio.new_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    
    return loop.run_until_complete(coro)

