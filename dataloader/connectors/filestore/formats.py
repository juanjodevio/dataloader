"""File format handlers for FileStore connector.

This module provides format handlers for reading and writing different file formats.
Supports CSV, JSON, JSONL, and Parquet formats, with extensibility for custom formats.
"""

import csv
import json
from abc import ABC, abstractmethod
from io import BytesIO, StringIO, TextIOWrapper
from typing import Any, Iterable

import pyarrow.parquet as pq

from dataloader.core.batch import ArrowBatch, Batch
from dataloader.core.exceptions import ConnectorError


class Format(ABC):
    """Base protocol for file format handlers.

    Format handlers are responsible for reading and writing files in a specific format.
    They convert between file content (bytes/strings) and Batch objects.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Return the format name (e.g., 'csv', 'json', 'parquet')."""
        ...

    @property
    @abstractmethod
    def extensions(self) -> list[str]:
        """Return list of file extensions for this format (e.g., ['.csv'], ['.json', '.jsonl'])."""
        ...

    @abstractmethod
    def read_batches(
        self,
        content: str | bytes,
        file_path: str,
        batch_size: int = 1000,
        encoding: str = "utf-8",
        **kwargs: Any,
    ) -> Iterable[ArrowBatch]:
        """Read batches from file content.

        Args:
            content: File content as string or bytes.
            file_path: Original file path (for metadata).
            batch_size: Maximum rows per batch.
            encoding: Text encoding (for text formats).
            **kwargs: Format-specific options.

        Yields:
            ArrowBatch instances containing the data.
        """
        ...

    @abstractmethod
    def write_batch(self, batch: Batch, encoding: str = "utf-8", **kwargs: Any) -> bytes:
        """Convert a batch to file content in this format.

        Args:
            batch: Batch to convert.
            encoding: Text encoding (for text formats).
            **kwargs: Format-specific options.

        Returns:
            File content as bytes.
        """
        ...


class CSVFormat(Format):
    """CSV format handler."""

    def __init__(self, delimiter: str = ",", has_header: bool = True):
        """Initialize CSV format handler.

        Args:
            delimiter: CSV delimiter character.
            has_header: Whether CSV files have a header row.
        """
        self._delimiter = delimiter
        self._has_header = has_header

    @property
    def name(self) -> str:
        return "csv"

    @property
    def extensions(self) -> list[str]:
        return [".csv"]

    def _infer_type(self, value: str) -> str:
        """Infer the type of a string value."""
        if not value or value.strip() == "":
            return "string"

        try:
            int(value)
            return "int"
        except ValueError:
            pass

        try:
            float(value)
            return "float"
        except ValueError:
            pass

        # Try common datetime formats
        from datetime import datetime

        datetime_formats = [
            "%Y-%m-%d",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%SZ",
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
        """Infer column types from sample rows."""
        if not sample_rows:
            return {col: "string" for col in columns}

        type_votes: dict[str, dict[str, int]] = {col: {} for col in columns}

        for row in sample_rows:
            for i, col in enumerate(columns):
                if i < len(row):
                    inferred = self._infer_type(row[i])
                    type_votes[col][inferred] = type_votes[col].get(inferred, 0) + 1

        schema = {}
        for col in columns:
            votes = type_votes[col]
            if not votes:
                schema[col] = "string"
                continue

            for preferred_type in ["datetime", "float", "int"]:
                if votes.get(preferred_type, 0) >= len(sample_rows) * 0.5:
                    schema[col] = preferred_type
                    break
            else:
                schema[col] = "string"

        return schema

    def read_batches(
        self,
        content: str | bytes,
        file_path: str,
        batch_size: int = 1000,
        encoding: str = "utf-8",
        **kwargs: Any,
    ) -> Iterable[DictBatch]:
        """Read batches from CSV content."""
        # Convert bytes to string if needed
        if isinstance(content, bytes):
            content_str = content.decode(encoding)
        else:
            content_str = content

        reader = csv.reader(StringIO(content_str), delimiter=self._delimiter)
        rows_buffer: list[list[str]] = []

        # Handle header
        if self._has_header:
            try:
                columns = next(reader)
            except StopIteration:
                return  # Empty file
        else:
            first_row = next(reader, None)
            if first_row is None:
                return
            columns = [f"col_{i}" for i in range(len(first_row))]
            rows_buffer.append(first_row)

        # Read all rows into buffer
        for row in reader:
            rows_buffer.append(row)

        # Infer schema from first 100 rows
        schema_sample = rows_buffer[: min(100, len(rows_buffer))]
        column_types = self._infer_schema(columns, schema_sample)

        # Yield in batches
        batch_number = 0
        for i in range(0, len(rows_buffer), batch_size):
            batch_rows = rows_buffer[i : i + batch_size]
            batch_number += 1

            yield ArrowBatch.from_rows(
                columns=columns,
                rows=batch_rows,
                metadata={
                    "batch_number": batch_number,
                    "row_count": len(batch_rows),
                    "format": "csv",
                    "file_path": file_path,
                    "column_types": column_types,
                },
            )

    def write_batch(self, batch: Batch, encoding: str = "utf-8", **kwargs: Any) -> bytes:
        """Convert batch to CSV string."""
        output = StringIO()
        writer = csv.writer(output, delimiter=self._delimiter)
        if self._has_header:
            writer.writerow(batch.columns)
        writer.writerows(batch.rows)
        return output.getvalue().encode(encoding)


class JSONFormat(Format):
    """JSON format handler (single JSON array or object)."""

    def __init__(self, **kwargs: Any):
        """Initialize JSON format handler.
        
        Args:
            **kwargs: Ignored (for compatibility with get_format).
        """
        pass

    @property
    def name(self) -> str:
        return "json"

    @property
    def extensions(self) -> list[str]:
        return [".json"]

    def read_batches(
        self,
        content: str | bytes,
        file_path: str,
        batch_size: int = 1000,
        encoding: str = "utf-8",
        **kwargs: Any,
    ) -> Iterable[ArrowBatch]:
        """Read batches from JSON content."""
        # Convert bytes to string if needed
        if isinstance(content, bytes):
            content_str = content.decode(encoding)
        else:
            content_str = content

        try:
            data = json.loads(content_str)
        except json.JSONDecodeError as e:
            raise ConnectorError(
                f"Failed to parse JSON: {e}",
                context={"file_path": file_path},
            ) from e

        # Handle both array and single object
        if isinstance(data, dict):
            rows = [data]
        elif isinstance(data, list):
            rows = data
        else:
            raise ConnectorError(
                f"JSON must be an object or array, got {type(data).__name__}",
                context={"file_path": file_path},
            )

        if not rows:
            return

        # Infer columns from first row
        first_row = rows[0]
        if not isinstance(first_row, dict):
            raise ConnectorError(
                "JSON array must contain objects",
                context={"file_path": file_path},
            )

        columns = list(first_row.keys())

        # Convert rows to list format
        row_data = [[row.get(col) for col in columns] for row in rows]

        # Yield in batches
        batch_number = 0
        for i in range(0, len(row_data), batch_size):
            batch_rows = row_data[i : i + batch_size]
            batch_number += 1

            yield ArrowBatch.from_rows(
                columns=columns,
                rows=batch_rows,
                metadata={
                    "batch_number": batch_number,
                    "row_count": len(batch_rows),
                    "format": "json",
                    "file_path": file_path,
                },
            )

    def write_batch(self, batch: Batch, encoding: str = "utf-8", **kwargs: Any) -> bytes:
        """Convert batch to JSON array."""
        # Convert rows to dict format
        rows = [
            {col: val for col, val in zip(batch.columns, row)} for row in batch.rows
        ]
        json_str = json.dumps(rows, indent=2, ensure_ascii=False)
        return json_str.encode(encoding)


class JSONLFormat(Format):
    """JSONL (JSON Lines) format handler - one JSON object per line."""

    def __init__(self, **kwargs: Any):
        """Initialize JSONL format handler.
        
        Args:
            **kwargs: Ignored (for compatibility with get_format).
        """
        pass

    @property
    def name(self) -> str:
        return "jsonl"

    @property
    def extensions(self) -> list[str]:
        return [".jsonl", ".ndjson"]

    def read_batches(
        self,
        content: str | bytes,
        file_path: str,
        batch_size: int = 1000,
        encoding: str = "utf-8",
        **kwargs: Any,
    ) -> Iterable[ArrowBatch]:
        """Read batches from JSONL content."""
        # Convert bytes to string if needed
        if isinstance(content, bytes):
            content_str = content.decode(encoding)
        else:
            content_str = content

        lines = content_str.strip().split("\n")
        if not lines or (len(lines) == 1 and not lines[0].strip()):
            return

        # Parse first line to get columns
        try:
            first_obj = json.loads(lines[0])
        except json.JSONDecodeError as e:
            raise ConnectorError(
                f"Failed to parse JSONL line 1: {e}",
                context={"file_path": file_path},
            ) from e

        if not isinstance(first_obj, dict):
            raise ConnectorError(
                "JSONL must contain JSON objects, one per line",
                context={"file_path": file_path},
            )

        columns = list(first_obj.keys())

        # Parse all lines
        rows: list[list[Any]] = []
        for line_num, line in enumerate(lines, 1):
            if not line.strip():
                continue
            try:
                obj = json.loads(line)
                if not isinstance(obj, dict):
                    raise ConnectorError(
                        f"JSONL line {line_num} is not an object",
                        context={"file_path": file_path, "line": line_num},
                    )
                rows.append([obj.get(col) for col in columns])
            except json.JSONDecodeError as e:
                raise ConnectorError(
                    f"Failed to parse JSONL line {line_num}: {e}",
                    context={"file_path": file_path, "line": line_num},
                ) from e

        # Yield in batches
        batch_number = 0
        for i in range(0, len(rows), batch_size):
            batch_rows = rows[i : i + batch_size]
            batch_number += 1

            yield ArrowBatch.from_rows(
                columns=columns,
                rows=batch_rows,
                metadata={
                    "batch_number": batch_number,
                    "row_count": len(batch_rows),
                    "format": "jsonl",
                    "file_path": file_path,
                },
            )

    def write_batch(self, batch: Batch, encoding: str = "utf-8", **kwargs: Any) -> bytes:
        """Convert batch to JSONL format (one JSON object per line)."""
        lines = []
        for row in batch.rows:
            obj = {col: val for col, val in zip(batch.columns, row)}
            lines.append(json.dumps(obj, ensure_ascii=False))
        return "\n".join(lines).encode(encoding)


class ParquetFormat(Format):
    """Parquet format handler using pandas."""

    def __init__(self, **kwargs: Any):
        """Initialize Parquet format handler.
        
        Args:
            **kwargs: Ignored (for compatibility with get_format).
        """
        pass

    @property
    def name(self) -> str:
        return "parquet"

    @property
    def extensions(self) -> list[str]:
        return [".parquet", ".pqt"]

    def read_batches(
        self,
        content: str | bytes,
        file_path: str,
        batch_size: int = 1000,
        encoding: str = "utf-8",
        **kwargs: Any,
    ) -> Iterable[ArrowBatch]:
        """Read batches from Parquet content using PyArrow."""
        # Parquet content should be bytes
        if isinstance(content, str):
            raise ConnectorError(
                "Parquet content must be bytes, not string",
                context={"file_path": file_path},
            )

        try:
            # Read parquet from bytes using PyArrow
            parquet_file = pq.ParquetFile(BytesIO(content))
            table = parquet_file.read(**kwargs)
        except Exception as e:
            raise ConnectorError(
                f"Failed to read Parquet: {e}",
                context={"file_path": file_path},
            ) from e

        columns = table.column_names
        column_types = {
            col: str(table.schema.field(col).type) for col in columns
        }

        # Yield in batches
        batch_number = 0
        for i in range(0, table.num_rows, batch_size):
            end_idx = min(i + batch_size, table.num_rows)
            batch_table = table.slice(i, end_idx - i)
            batch_number += 1

            yield ArrowBatch(
                batch_table,
                metadata={
                    "batch_number": batch_number,
                    "row_count": batch_table.num_rows,
                    "format": "parquet",
                    "file_path": file_path,
                    "column_types": column_types,
                },
            )

    def write_batch(self, batch: ArrowBatch, encoding: str = "utf-8", **kwargs: Any) -> bytes:
        """Convert batch to Parquet format using PyArrow."""
        try:
            # Get Arrow table from batch
            arrow_table = batch.to_arrow()

            # Write to bytes buffer
            buffer = BytesIO()
            pq.write_table(arrow_table, buffer, **kwargs)
            return buffer.getvalue()
        except Exception as e:
            raise ConnectorError(
                f"Failed to write Parquet: {e}",
            ) from e


# Format registry for custom formats
_format_registry: dict[str, type[Format]] = {}


def register_format(format_class: type[Format]) -> type[Format]:
    """Register a custom format handler.

    Args:
        format_class: Format class to register (must have a 'name' property).

    Returns:
        The format class (for use as decorator).

    Example:
        @register_format
        class CustomFormat(Format):
            @property
            def name(self) -> str:
                return "custom"
            ...
    """
    format_instance = format_class()
    format_name = format_instance.name
    _format_registry[format_name] = format_class
    return format_class


def get_format(format_name: str, **kwargs: Any) -> Format:
    """Get a format handler by name.

    Args:
        format_name: Format name (e.g., 'csv', 'json', 'parquet').
        **kwargs: Format-specific initialization options.

    Returns:
        Format handler instance.

    Raises:
        ConnectorError: If format is not supported.
    """
    format_name_lower = format_name.lower()

    # Built-in formats
    if format_name_lower == "csv":
        return CSVFormat(**kwargs)
    elif format_name_lower == "json":
        return JSONFormat(**kwargs)
    elif format_name_lower == "jsonl":
        return JSONLFormat(**kwargs)
    elif format_name_lower == "parquet":
        return ParquetFormat(**kwargs)
    elif format_name_lower in _format_registry:
        # Custom format
        return _format_registry[format_name_lower](**kwargs)
    else:
        available = ", ".join(["csv", "json", "jsonl", "parquet"] + list(_format_registry.keys()))
        raise ConnectorError(
            f"Unsupported format: '{format_name}'. Available formats: {available}",
            context={"format": format_name, "available": available},
        )


def list_formats() -> list[str]:
    """List all available format names."""
    builtin = ["csv", "json", "jsonl", "parquet"]
    custom = list(_format_registry.keys())
    return sorted(builtin + custom)

