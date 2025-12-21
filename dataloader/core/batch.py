"""Batch protocol and implementation for data batches."""

from typing import Any, Protocol

import pyarrow as pa


class Batch(Protocol):
    """Protocol defining the interface for data batches.

    Batches use Apache Arrow format for efficient processing:
    - Zero-copy data transfer between connectors
    - Memory-efficient operations
    - Better integration with Arrow-based tools (DuckDB, Polars, etc.)
    """

    @property
    def columns(self) -> list[str]:
        """Return the column names for this batch."""
        ...

    @property
    def rows(self) -> list[list[Any]]:
        """Return the row data for this batch."""
        ...

    @property
    def metadata(self) -> dict[str, Any]:
        """Return metadata associated with this batch."""
        ...

    @property
    def row_count(self) -> int:
        """Return the number of rows in this batch."""
        ...

    def to_dict(self) -> dict[str, Any]:
        """Convert batch to dictionary representation.

        Returns:
            Dictionary with keys: columns, rows, metadata
        """
        ...


class ArrowBatch:
    """Arrow-based batch implementation using PyArrow.

    Uses PyArrow Table for efficient data handling and zero-copy operations.
    """

    def __init__(self, table: pa.Table, metadata: dict[str, Any] | None = None):
        """Initialize from Arrow table.

        Args:
            table: PyArrow Table containing the data
            metadata: Optional metadata dictionary

        Raises:
            ValueError: If table has zero columns
        """
        if len(table.column_names) == 0:
            raise ValueError("table cannot have zero columns")
        
        self._table = table
        self._metadata = metadata or {}

    @classmethod
    def from_rows(
        cls,
        columns: list[str],
        rows: list[list[Any]],
        metadata: dict[str, Any] | None = None,
    ) -> "ArrowBatch":
        """Create ArrowBatch from columns and rows.

        Args:
            columns: List of column names
            rows: List of row data (each row is a list of values)
            metadata: Optional metadata dictionary

        Returns:
            ArrowBatch instance

        Raises:
            ValueError: If columns is empty or row lengths don't match column count
        """
        if len(columns) == 0:
            raise ValueError("columns cannot be empty")
        
        if rows:
            # Validate row lengths
            for i, row in enumerate(rows):
                if len(row) != len(columns):
                    raise ValueError(
                        f"Row {i} length {len(row)} does not match column count {len(columns)}"
                    )
            
            # Convert rows (list of lists) to list of dicts for PyArrow
            # Each row becomes a dict with column names as keys
            row_dicts = [dict(zip(columns, row)) for row in rows]
            
            # Create Arrow table from list of dicts (PyArrow infers types)
            table = pa.Table.from_pylist(row_dicts, schema=None)
        else:
            # Empty batch - create table with empty arrays
            # Use pa.null() type as placeholder (will be inferred on first data)
            empty_arrays = [pa.array([], type=pa.null()) for _ in columns]
            table = pa.Table.from_arrays(empty_arrays, names=columns)
        
        return cls(table, metadata)

    @property
    def columns(self) -> list[str]:
        """Return column names from Arrow schema."""
        return self._table.column_names

    @property
    def rows(self) -> list[list[Any]]:
        """Return rows as list of lists (efficient conversion).

        Converts Arrow table to list of lists maintaining column order.
        """
        # Convert to list of dicts first (PyArrow's efficient method)
        dict_list = self._table.to_pylist()
        
        # Convert list of dicts to list of lists in column order
        column_names = self.columns
        return [[row[col] for col in column_names] for row in dict_list]

    @property
    def row_count(self) -> int:
        """Return number of rows."""
        return len(self._table)

    @property
    def metadata(self) -> dict[str, Any]:
        """Return metadata dictionary."""
        return self._metadata

    def to_arrow(self) -> pa.Table:
        """Return underlying Arrow table for zero-copy operations.

        Returns:
            PyArrow Table instance
        """
        return self._table

    def to_dict(self) -> dict[str, Any]:
        """Convert to dict representation (for debugging/compatibility).

        Returns:
            Dictionary with keys: columns, rows, metadata
        """
        return {
            "columns": self.columns,
            "rows": self.rows,
            "metadata": self.metadata,
        }
