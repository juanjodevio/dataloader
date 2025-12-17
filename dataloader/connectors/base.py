"""Base protocols for source and destination connectors.

This module defines the core abstractions for data connectors:
- Source: Protocol for reading data in batches
- Destination: Protocol for writing batches
"""

from typing import Iterable, Protocol, runtime_checkable

from dataloader.core.batch import Batch
from dataloader.core.state import State


@runtime_checkable
class Source(Protocol):
    """Protocol for data source connectors.

    Sources read data in batches, optionally using state for incremental reads.
    Each implementation manages its own connection lifecycle internally.

    Example:
        class PostgresSource:
            def read_batches(self, state: State) -> Iterable[Batch]:
                cursor_value = state.cursor_values.get("updated_at")
                # ... execute query with cursor filter
                for chunk in self._fetch_chunks():
                    yield DictBatch(...)
    """

    def read_batches(self, state: State) -> Iterable[Batch]:
        """Read data from source as an iterator of batches.

        Args:
            state: Current state containing cursor values for incremental reads.
                   Use state.cursor_values to access the last processed cursor.

        Yields:
            Batch instances containing the data.

        Raises:
            ConnectorError: If connection or read operation fails.
        """
        ...


@runtime_checkable
class Destination(Protocol):
    """Protocol for data destination connectors.

    Destinations write batches of data, optionally updating state for checkpoints.
    Each implementation manages its own connection lifecycle internally.

    Example:
        class DuckDBDestination:
            def write_batch(self, batch: Batch, state: State) -> None:
                # ... insert rows into DuckDB table
                pass
    """

    def write_batch(self, batch: Batch, state: State) -> None:
        """Write a single batch to the destination.

        Args:
            batch: The batch of data to write.
            state: Current state (can be used for write checkpoints).

        Raises:
            ConnectorError: If connection or write operation fails.
        """
        ...

