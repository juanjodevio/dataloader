"""CSV connector for reading and writing CSV files."""

import csv
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Union

from dataloader.connectors.registry import ConnectorConfigUnion, register_connector
from dataloader.core.batch import Batch, DictBatch
from dataloader.core.exceptions import ConnectorError
from dataloader.core.state import State
from dataloader.models.destination_config import DestinationConfig
from dataloader.models.source_config import SourceConfig

from .config import CSVConnectorConfig


class CSVConnector:
    """Unified connector for CSV files supporting both read and write operations.

    Reads CSV files in batches, supporting header detection and basic
    type inference. For incremental loads, filters rows after reading
    based on cursor column comparison. Writes batches to CSV files
    with configurable formatting options.
    """

    DEFAULT_BATCH_SIZE = 1000
    DEFAULT_ENCODING = "utf-8"

    def __init__(
        self,
        config: Union[CSVConnectorConfig, SourceConfig, DestinationConfig],
        connection: dict[str, Any] | None = None,
    ):
        """Initialize CSVConnector.

        Args:
            config: CSV connector configuration (CSVConnectorConfig, SourceConfig, or DestinationConfig).
            connection: Optional connection parameters (legacy, defaults to empty dict).
                All configuration should be in the config parameter. This parameter is kept
                for backward compatibility with the factory signature.
        """
        self._config = config
        self._connection = connection or {}

        # Extract config values (support both new and legacy configs)
        if isinstance(config, CSVConnectorConfig):
            self._path = config.path
            self._write_mode = config.write_mode
        elif isinstance(config, SourceConfig):
            self._path = config.path or ""
            self._write_mode = "append"  # Default for source configs
        else:  # DestinationConfig
            self._path = config.path or ""
            self._write_mode = config.write_mode

        # Reading configuration
        self._batch_size = self._connection.get("batch_size", self.DEFAULT_BATCH_SIZE)
        self._encoding = self._connection.get("encoding", self.DEFAULT_ENCODING)
        self._delimiter = self._connection.get("delimiter", ",")
        self._has_header = self._connection.get("has_header", True)

        # Writing state
        self._file_initialized = False

    # ========== Reading methods ==========

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

    def _infer_schema(
        self, columns: list[str], sample_rows: list[list[str]]
    ) -> dict[str, str]:
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
        self,
        row: list[str],
        columns: list[str],
        cursor_column: str,
        cursor_value: Any,
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
        file_path = Path(self._path)

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
                incremental = getattr(self._config, "incremental", None)
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

    # ========== Writing methods ==========

    def _ensure_file_initialized(self, batch: Batch) -> None:
        """Ensure CSV file is initialized for writing.

        For overwrite mode, truncates the file on first write. For append mode,
        creates the file if it doesn't exist and writes header if needed.
        """
        if self._file_initialized:
            return

        file_path = Path(self._path)

        if self._write_mode == "overwrite":
            # Truncate file if it exists, create new file with header
            with open(file_path, "w", encoding=self._encoding, newline="") as f:
                writer = csv.writer(f, delimiter=self._delimiter)
                if self._has_header:
                    writer.writerow(batch.columns)
            self._file_initialized = True

        elif self._write_mode == "append":
            # Check if file exists
            file_exists = file_path.exists() and file_path.stat().st_size > 0
            if not file_exists:
                # Create new file with header
                with open(file_path, "w", encoding=self._encoding, newline="") as f:
                    writer = csv.writer(f, delimiter=self._delimiter)
                    if self._has_header:
                        writer.writerow(batch.columns)
            # If file exists, assume it already has a header (if header is enabled)
            # and we'll append rows to it
            self._file_initialized = True

    def write_batch(self, batch: Batch, state: State) -> None:
        """Write a batch to CSV file.

        Creates the file if it doesn't exist, handles write modes (append/overwrite),
        and writes rows with proper CSV formatting.

        Args:
            batch: Batch of data to write.
            state: Current pipeline state (unused for CSV but kept for protocol).

        Raises:
            ConnectorError: If file writing fails.
        """
        if not batch.rows:
            return

        file_path = Path(self._path)

        try:
            # Ensure file is initialized (create/truncate, write header if needed)
            self._ensure_file_initialized(batch)

            # Open file in append mode (file already exists with header if needed)
            with open(file_path, "a", encoding=self._encoding, newline="") as f:
                writer = csv.writer(f, delimiter=self._delimiter)
                writer.writerows(batch.rows)

        except OSError as e:
            raise ConnectorError(
                f"Failed to write CSV file: {e}",
                context={"path": str(file_path)},
            ) from e


@register_connector("csv")
def create_csv_connector(
    config: ConnectorConfigUnion, connection: dict[str, Any] | None = None
) -> CSVConnector:
    """Factory function for creating CSVConnector instances."""
    return CSVConnector(config, connection)

