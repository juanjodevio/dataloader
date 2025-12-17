"""Batch protocol and implementation for data batches."""

from typing import Any, Protocol


class Batch(Protocol):
    """Protocol defining the interface for data batches.

    For v0.1, batches use a simple dict-based structure:
    {
        "columns": ["id", "name", "updated_at"],
        "rows": [[1, "Alice", "2024-01-01"], ...],
        "metadata": {"batch_id": "...", "row_count": 100}
    }
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


class DictBatch:
    """Simple dict-based batch implementation for v0.1."""

    def __init__(
        self,
        columns: list[str],
        rows: list[list[Any]],
        metadata: dict[str, Any] | None = None,
    ):
        """Initialize a batch from columns, rows, and optional metadata.

        Args:
            columns: List of column names
            rows: List of row data (each row is a list of values)
            metadata: Optional metadata dictionary
        """
        if len(columns) == 0:
            raise ValueError("columns cannot be empty")
        if rows:
            for i, row in enumerate(rows):
                if len(row) != len(columns):
                    raise ValueError(
                        f"Row {i} length {len(row)} does not match column count {len(columns)}"
                    )

        self._columns = columns
        self._rows = rows
        self._metadata = metadata or {}

    @property
    def columns(self) -> list[str]:
        """Return the column names for this batch."""
        return self._columns

    @property
    def rows(self) -> list[list[Any]]:
        """Return the row data for this batch."""
        return self._rows

    @property
    def metadata(self) -> dict[str, Any]:
        """Return metadata associated with this batch."""
        return self._metadata

    @property
    def row_count(self) -> int:
        """Return the number of rows in this batch."""
        return len(self._rows)

    def to_dict(self) -> dict[str, Any]:
        """Convert batch to dictionary representation.

        Returns:
            Dictionary with keys: columns, rows, metadata
        """
        return {
            "columns": self._columns,
            "rows": self._rows,
            "metadata": self._metadata,
        }

