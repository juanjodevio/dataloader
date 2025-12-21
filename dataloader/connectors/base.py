"""Base protocols for data connectors.

This module defines the core abstractions for data connectors:
- Connector: Unified protocol for connectors that can read and/or write data
"""

from typing import Iterable, Protocol, runtime_checkable

from dataloader.core.batch import Batch
from dataloader.core.state import State


@runtime_checkable
class Connector(Protocol):
    """Unified protocol for connectors that can read and/or write data.

    Connectors can implement read_batches(), write_batch(), or both.
    Each implementation manages its own connection lifecycle internally.

    Example:
        class PostgresConnector:
            def read_batches(self, state: State) -> Iterable[Batch]:
                # ... execute query with cursor filter
                for chunk in self._fetch_chunks():
                    yield DictBatch(...)
            
            def write_batch(self, batch: Batch, state: State) -> None:
                # ... insert rows into database
                pass
    """

    def read_batches(self, state: State) -> Iterable[Batch]:
        """Read data from source as an iterator of batches.

        Connectors that don't support reading should raise NotImplementedError.

        Args:
            state: Current state containing cursor values for incremental reads.
                   Use state.cursor_values to access the last processed cursor.

        Yields:
            Batch instances containing the data.

        Raises:
            NotImplementedError: If the connector doesn't support reading.
            ConnectorError: If connection or read operation fails.
        """
        ...

    def write_batch(self, batch: Batch, state: State) -> None:
        """Write a single batch to the destination.

        Connectors that don't support writing should raise NotImplementedError.

        Args:
            batch: The batch of data to write.
            state: Current state (can be used for write checkpoints).

        Raises:
            NotImplementedError: If the connector doesn't support writing.
            ConnectorError: If connection or write operation fails.
        """
        ...

