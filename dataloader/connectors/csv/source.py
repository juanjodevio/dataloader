"""CSV source connector for reading data in batches."""

import csv
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

from dataloader.connectors.registry import register_source
from dataloader.core.batch import DictBatch
from dataloader.core.exceptions import ConnectorError
from dataloader.core.state import State
from dataloader.models.source_config import SourceConfig


class CSVSource:
    """Source connector for CSV files.

    Reads CSV files in batches, supporting header detection and basic
    type inference. For incremental loads, filters rows after reading
    based on cursor column comparison.
    """

    DEFAULT_BATCH_SIZE = 1000
    DEFAULT_ENCODING = "utf-8"

    def __init__(self, config: SourceConfig, connection: dict[str, Any]):
        """Initialize CSVSource.

        Args:
            config: Source configuration containing path and incremental settings.
            connection: Connection parameters (encoding, delimiter, batch_size, etc.).
        """
        self._config = config
        self._connection = connection
        self._batch_size = connection.get("batch_size", self.DEFAULT_BATCH_SIZE)
        self._encoding = connection.get("encoding", self.DEFAULT_ENCODING)
        self._delimiter = connection.get("delimiter", ",")
        self._has_header = connection.get("has_header", True)

    def _infer_type(self, value: str) -> str:
        """Infer the type of a string value.

        Args:
            value: String value to analyze.

        Returns:
            Type name: 'int', 'float', 'datetime', or 'string'.
        """
        if not value or value.strip() == "":
            return "string"

        # Try int
        try:
            int(value)
            return "int"
        except ValueError:
            pass

        # Try float
        try:
            float(value)
            return "float"
        except ValueError:
            pass

        # Try common datetime formats
        datetime_formats = [
            "%Y-%m-%d",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%SZ",
            "%d/%m/%Y",
            "%m/%d/%Y",
        ]
        for fmt in datetime_formats:
            try:
                datetime.strptime(value, fmt)
                return "datetime"
            except ValueError:
                continue

        return "string"

    def _infer_schema(self, columns: list[str], sample_rows: list[list[str]]) -> dict[str, str]:
        """Infer column types from sample rows.

        Args:
            columns: Column names.
            sample_rows: Sample rows for type inference.

        Returns:
            Dictionary mapping column names to inferred types.
        """
        if not sample_rows:
            return {col: "string" for col in columns}

        # Collect type votes for each column
        type_votes: dict[str, dict[str, int]] = {col: {} for col in columns}

        for row in sample_rows:
            for i, col in enumerate(columns):
                if i < len(row):
                    inferred = self._infer_type(row[i])
                    type_votes[col][inferred] = type_votes[col].get(inferred, 0) + 1

        # Pick the most common non-string type for each column
        schema = {}
        for col in columns:
            votes = type_votes[col]
            if not votes:
                schema[col] = "string"
                continue

            # Prefer specific types over string if they have majority
            for preferred_type in ["datetime", "float", "int"]:
                if votes.get(preferred_type, 0) >= len(sample_rows) * 0.5:
                    schema[col] = preferred_type
                    break
            else:
                schema[col] = "string"

        return schema

    def _should_include_row(
        self, row: list[str], columns: list[str], cursor_column: str, cursor_value: Any
    ) -> bool:
        """Check if row should be included based on cursor filtering.

        Args:
            row: Row data as list of strings.
            columns: Column names.
            cursor_column: Name of cursor column.
            cursor_value: Last processed cursor value.

        Returns:
            True if row should be included, False otherwise.
        """
        if cursor_column not in columns:
            return True

        idx = columns.index(cursor_column)
        if idx >= len(row):
            return True

        row_value = row[idx]

        # Simple string/numeric comparison
        try:
            # Try numeric comparison
            return float(row_value) > float(cursor_value)
        except (ValueError, TypeError):
            # Fall back to string comparison
            return str(row_value) > str(cursor_value)

    def read_batches(self, state: State) -> Iterable[DictBatch]:
        """Read CSV file as batches.

        Reads the file in chunks of batch_size rows, applying cursor-based
        filtering for incremental loads after reading.

        Args:
            state: Current state containing cursor values for incremental reads.

        Yields:
            DictBatch instances containing the data.

        Raises:
            ConnectorError: If file reading fails.
        """
        file_path = Path(self._config.path)

        if not file_path.exists():
            raise ConnectorError(
                f"CSV file not found: {file_path}",
                context={"path": str(file_path)},
            )

        try:
            with open(file_path, "r", encoding=self._encoding, newline="") as f:
                reader = csv.reader(f, delimiter=self._delimiter)
                rows_buffer: list[list[str]] = []

                # Handle header
                if self._has_header:
                    try:
                        columns = next(reader)
                    except StopIteration:
                        raise ConnectorError(
                            "CSV file is empty",
                            context={"path": str(file_path)},
                        )
                else:
                    # Peek first row to determine column count
                    first_row = next(reader, None)
                    if first_row is None:
                        raise ConnectorError(
                            "CSV file is empty",
                            context={"path": str(file_path)},
                        )
                    columns = [f"col_{i}" for i in range(len(first_row))]
                    # Need to process this row
                    rows_buffer.append(first_row)

                # Get incremental config
                incremental = self._config.incremental
                cursor_column = incremental.cursor_column if incremental else None
                cursor_value = (
                    state.cursor_values.get(cursor_column) if cursor_column else None
                )

                # Schema will be inferred from first batch
                column_types: dict[str, str] = {}
                batch_number = 0

                while True:
                    # Fill batch buffer
                    while len(rows_buffer) < self._batch_size:
                        try:
                            row = next(reader)
                            rows_buffer.append(row)
                        except StopIteration:
                            break

                    if not rows_buffer:
                        break

                    # Infer schema from first batch
                    if batch_number == 0:
                        schema_sample = rows_buffer[: min(100, len(rows_buffer))]
                        column_types = self._infer_schema(columns, schema_sample)

                    # Apply cursor filtering for incremental loads
                    if cursor_column and cursor_value is not None:
                        filtered_rows = [
                            row
                            for row in rows_buffer
                            if self._should_include_row(
                                row, columns, cursor_column, cursor_value
                            )
                        ]
                    else:
                        filtered_rows = rows_buffer

                    # Clear buffer for next iteration
                    rows_buffer = []

                    # Skip empty batches after filtering
                    if not filtered_rows:
                        continue

                    batch_number += 1
                    yield DictBatch(
                        columns=columns,
                        rows=filtered_rows,
                        metadata={
                            "batch_number": batch_number,
                            "row_count": len(filtered_rows),
                            "source_type": "csv",
                            "file_path": str(file_path),
                            "column_types": column_types,
                        },
                    )

        except OSError as e:
            raise ConnectorError(
                f"Failed to read CSV file: {e}",
                context={"path": str(file_path)},
            ) from e


@register_source("csv")
def create_csv_source(config: SourceConfig, connection: dict[str, Any]) -> CSVSource:
    """Factory function for creating CSVSource instances."""
    return CSVSource(config, connection)
