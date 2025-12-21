"""Unified FileStore connector for reading and writing files across multiple storage backends.

Uses fsspec for abstraction, supporting S3, local filesystem, Azure, GCS, and other backends.
"""

import csv
from datetime import datetime
from io import StringIO
from pathlib import Path
from typing import Any, Iterable, Union

import fsspec
from fsspec import AbstractFileSystem

from dataloader.connectors.registry import ConnectorConfigUnion, register_connector
from dataloader.core.batch import Batch, DictBatch
from dataloader.core.exceptions import ConnectorError
from dataloader.core.state import State
from dataloader.models.destination_config import DestinationConfig
from dataloader.models.source_config import SourceConfig

from .config import FileStoreConfigType, LocalFileStoreConfig, S3FileStoreConfig

DEFAULT_BATCH_SIZE = 1000
DEFAULT_ENCODING = "utf-8"


class FileStoreConnector:
    """Unified connector for file-based storage backends using fsspec.

    Supports multiple storage backends (S3, local filesystem, Azure, GCS, etc.)
    and multiple file formats (CSV, Parquet, JSONL). Uses fsspec for unified
    file operations across all backends.
    """

    def __init__(
        self,
        config: Union[FileStoreConfigType, SourceConfig, DestinationConfig],
        connection: dict[str, Any] | None = None,
    ):
        """Initialize FileStoreConnector.

        Args:
            config: FileStore connector configuration (FileStoreConfigType, SourceConfig, or DestinationConfig).
            connection: Optional connection parameters (legacy, defaults to empty dict).
                All configuration should be in the config parameter. This parameter is kept
                for backward compatibility with the factory signature.
        """
        self._config = config
        self._connection = connection or {}

        # Extract config values (support both new and legacy configs)
        if isinstance(config, (S3FileStoreConfig, LocalFileStoreConfig)):
            self._backend = getattr(config, "backend", "local")
            self._path = config.path
            self._format = config.format
            self._write_mode = config.write_mode
        elif isinstance(config, SourceConfig):
            # Infer backend from path or config
            self._backend = self._infer_backend_from_config(config)
            self._path = config.path or ""
            self._format = "csv"  # Default format
            self._write_mode = "append"
        else:  # DestinationConfig
            self._backend = self._infer_backend_from_config(config)
            self._path = config.path or ""
            self._format = "csv"  # Default format
            self._write_mode = config.write_mode

        # Reading configuration
        self._batch_size = self._connection.get("batch_size", DEFAULT_BATCH_SIZE)
        self._encoding = self._connection.get("encoding", DEFAULT_ENCODING)
        self._delimiter = self._connection.get("delimiter", ",")
        self._has_header = self._connection.get("has_header", True)

        # Writing state
        self._file_initialized = False
        self._batch_counter = 0
        self._written_files: list[str] = []

        # Build fsspec storage options
        self._storage_options = self._build_storage_options(config, self._connection)
        self._filesystem: AbstractFileSystem | None = None

    def _infer_backend_from_config(
        self, config: Union[SourceConfig, DestinationConfig]
    ) -> str:
        """Infer storage backend from config path or type."""
        path = getattr(config, "path", "") or ""

        # Check for URL schemes
        if path.startswith("s3://"):
            return "s3"
        elif path.startswith("gs://"):
            return "gcs"
        elif path.startswith("az://") or path.startswith("abfss://"):
            return "azure"
        elif path.startswith("file://") or not any(
            path.startswith(scheme) for scheme in ["s3://", "gs://", "az://", "abfss://"]
        ):
            return "local"

        # Fallback: check if config has bucket (S3) or other backend-specific fields
        if hasattr(config, "bucket") and getattr(config, "bucket"):
            return "s3"

        return "local"  # Default to local

    def _build_storage_options(
        self,
        config: Union[FileStoreConfigType, SourceConfig, DestinationConfig],
        connection: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Build fsspec storage options for the configured backend."""
        storage_options: dict[str, Any] = {}
        conn = connection or {}

        # S3-specific options
        if self._backend == "s3":
            access_key = (
                conn.get("aws_access_key_id")
                or getattr(config, "access_key", None)
                or conn.get("access_key")
            )
            secret_key = (
                conn.get("aws_secret_access_key")
                or getattr(config, "secret_key", None)
                or conn.get("secret_key")
            )
            region = (
                conn.get("region_name")
                or getattr(config, "region", None)
                or conn.get("region")
            )

            if access_key and secret_key:
                storage_options["key"] = access_key
                storage_options["secret"] = secret_key

            if region:
                storage_options["client_kwargs"] = {"region_name": region}

            # Support custom endpoint (LocalStack, MinIO)
            if conn.get("endpoint_url"):
                if "client_kwargs" not in storage_options:
                    storage_options["client_kwargs"] = {}
                storage_options["client_kwargs"]["endpoint_url"] = conn["endpoint_url"]

        # Add other backend-specific options here (GCS, Azure, etc.)
        # For now, local filesystem needs no special options

        return storage_options

    def _build_file_url(self, file_path: str) -> str:
        """Build full file URL for the configured backend."""
        if self._backend == "s3":
            # For S3, path should be bucket/path or we extract from config
            if isinstance(self._config, S3FileStoreConfig):
                bucket = self._config.bucket
                # Remove leading slash from path
                key = file_path.lstrip("/")
                return f"s3://{bucket}/{key}"
            elif file_path.startswith("s3://"):
                return file_path
            else:
                # Try to infer bucket from path or config
                # For legacy SourceConfig/DestinationConfig with bucket field
                bucket = getattr(self._config, "bucket", None) or ""
                if bucket:
                    key = file_path.lstrip("/")
                    return f"s3://{bucket}/{key}"
                return f"s3://{file_path}"
        elif self._backend == "local":
            # For local, convert to file:// URL or use as-is
            if file_path.startswith("file://"):
                return file_path
            elif Path(file_path).is_absolute():
                return f"file://{file_path}"
            else:
                return file_path  # Relative path, use as-is
        elif self._backend == "gcs":
            if not file_path.startswith("gs://"):
                return f"gs://{file_path}"
            return file_path
        elif self._backend == "azure":
            if not file_path.startswith(("az://", "abfss://")):
                return f"az://{file_path}"
            return file_path
        else:
            return file_path

    def _get_filesystem(self) -> AbstractFileSystem:
        """Get or create fsspec filesystem instance."""
        if self._filesystem is None:
            try:
                protocol = self._backend if self._backend != "local" else "file"
                self._filesystem = fsspec.filesystem(protocol, **self._storage_options)
            except Exception as e:
                raise ConnectorError(
                    f"Failed to create filesystem for backend '{self._backend}': {e}",
                    context={"backend": self._backend, "path": self._path},
                ) from e
        return self._filesystem

    # ========== Reading methods ==========

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

    def _list_files(self) -> list[dict[str, Any]]:
        """List files in the configured path using fsspec."""
        fs = self._get_filesystem()
        file_url = self._build_file_url(self._path)

        try:
            # For directories, list all files
            if fs.isdir(file_url):
                files = []
                for file_path in fs.find(file_url):
                    # Filter by format extension
                    if self._format == "csv" and file_path.endswith(".csv"):
                        files.append({"path": file_path, "name": Path(file_path).name})
                    elif self._format == "parquet" and file_path.endswith(".parquet"):
                        files.append({"path": file_path, "name": Path(file_path).name})
                    elif self._format == "jsonl" and file_path.endswith(".jsonl"):
                        files.append({"path": file_path, "name": Path(file_path).name})
                return files
            else:
                # Single file
                return [{"path": file_url, "name": Path(file_url).name}]
        except Exception as e:
            raise ConnectorError(
                f"Failed to list files: {e}",
                context={"path": file_url, "backend": self._backend},
            ) from e

    def _filter_files_by_state(
        self, files: list[dict[str, Any]], state: State
    ) -> list[dict[str, Any]]:
        """Filter files based on incremental state."""
        last_modified_cursor = state.cursor_values.get("last_modified")
        last_file_cursor = state.cursor_values.get("last_file")

        if not last_modified_cursor and not last_file_cursor:
            return files

        filtered = []
        fs = self._get_filesystem()

        for file_info in files:
            file_path = file_info["path"]

            # Filter by modification time
            if last_modified_cursor:
                try:
                    mod_time = fs.modified(file_path)
                    if isinstance(mod_time, datetime):
                        mod_time_str = mod_time.isoformat()
                    else:
                        mod_time_str = str(mod_time)

                    if mod_time_str <= str(last_modified_cursor):
                        continue
                except Exception:
                    # If we can't get mod time, include the file
                    pass

            # Filter by file name (lexicographic order)
            if last_file_cursor and file_path <= str(last_file_cursor):
                continue

            filtered.append(file_info)

        return filtered

    def _read_csv_batches(
        self, content: str, file_path: str, file_metadata: dict[str, Any] | None = None
    ) -> Iterable[DictBatch]:
        """Read batches from CSV content string."""
        reader = csv.reader(StringIO(content), delimiter=self._delimiter)
        rows_buffer: list[list[str]] = []

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
        for i in range(0, len(rows_buffer), self._batch_size):
            batch_rows = rows_buffer[i : i + self._batch_size]
            batch_number += 1

            yield DictBatch(
                columns=columns,
                rows=batch_rows,
                metadata={
                    "batch_number": batch_number,
                    "row_count": len(batch_rows),
                    "source_type": "filestore",
                    "backend": self._backend,
                    "file_path": file_path,
                    "format": self._format,
                    "column_types": column_types,
                },
            )

    def read_batches(self, state: State) -> Iterable[DictBatch]:
        """Read files from FileStore as batches.

        Uses fsspec for unified file operations across all backends.

        Args:
            state: Current state containing cursor values for incremental reads.

        Yields:
            DictBatch instances containing the data.

        Raises:
            NotImplementedError: If reading is not supported (should not happen for FileStore).
            ConnectorError: If file operations fail.
        """
        if self._format != "csv":
            raise NotImplementedError(
                f"Format '{self._format}' is not yet supported. Only 'csv' is supported in v0.1."
            )

        try:
            # List files
            files = self._list_files()
            files = self._filter_files_by_state(files, state)

            # Sort by path for consistent processing order
            files.sort(key=lambda x: x["path"])

            # Read each file
            fs = self._get_filesystem()
            for file_info in files:
                file_path = file_info["path"]

                try:
                    # Read file using fsspec
                    with fs.open(file_path, mode="r", encoding=self._encoding) as f:
                        content = f.read()
                    yield from self._read_csv_batches(content, file_path, file_info)
                except Exception as e:
                    raise ConnectorError(
                        f"Failed to read file: {e}",
                        context={"path": file_path, "backend": self._backend},
                    ) from e

        except ConnectorError:
            raise
        except Exception as e:
            raise ConnectorError(
                f"Unexpected error reading from FileStore: {e}",
                context={"path": self._path, "backend": self._backend},
            ) from e

    # ========== Writing methods ==========

    def _generate_file_path(self, batch: Batch) -> str:
        """Generate file path for the batch."""
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        self._batch_counter += 1

        # Build filename based on format
        if self._format == "csv":
            ext = ".csv"
        elif self._format == "parquet":
            ext = ".parquet"
        elif self._format == "jsonl":
            ext = ".jsonl"
        else:
            ext = ".csv"  # Default

        filename = f"data_{timestamp}_{self._batch_counter:04d}{ext}"

        # Build full path
        base_path = self._path.rstrip("/")
        if base_path:
            return f"{base_path}/{filename}"
        return filename

    def _batch_to_csv(self, batch: Batch) -> str:
        """Convert batch to CSV string."""
        output = StringIO()
        writer = csv.writer(output, delimiter=self._delimiter)
        if self._has_header:
            writer.writerow(batch.columns)
        writer.writerows(batch.rows)
        return output.getvalue()

    def _delete_existing_files(self) -> None:
        """Delete existing files at the destination path (for overwrite mode)."""
        fs = self._get_filesystem()
        file_url = self._build_file_url(self._path)

        try:
            if fs.isdir(file_url):
                # List and delete all files matching the format
                for file_path in fs.find(file_url):
                    if file_path.endswith(f".{self._format}"):
                        fs.rm(file_path)
            elif fs.exists(file_url):
                # Single file, delete it
                fs.rm(file_url)
        except Exception as e:
            raise ConnectorError(
                f"Failed to delete existing files: {e}",
                context={"path": file_url, "backend": self._backend},
            ) from e

    def write_batch(self, batch: Batch, state: State) -> None:
        """Write a batch to FileStore.

        Args:
            batch: Batch of data to write.
            state: Current pipeline state.

        Raises:
            NotImplementedError: If writing is not supported (should not happen for FileStore).
            ConnectorError: If file operations fail.
        """
        if self._format != "csv":
            raise NotImplementedError(
                f"Format '{self._format}' is not yet supported for writing. Only 'csv' is supported in v0.1."
            )

        if self._write_mode == "merge":
            raise ConnectorError(
                "Merge write mode is not supported for FileStore in v0.1. Use 'append' or 'overwrite'.",
                context={"path": self._path, "write_mode": self._write_mode},
            )

        # Handle overwrite on first batch
        if self._write_mode == "overwrite" and self._batch_counter == 0:
            self._delete_existing_files()

        if not batch.rows:
            return

        # Generate file path and convert to CSV
        file_path = self._generate_file_path(batch)
        file_url = self._build_file_url(file_path)
        csv_content = self._batch_to_csv(batch)

        # Write using fsspec
        fs = self._get_filesystem()
        try:
            with fs.open(file_url, mode="wb") as f:
                f.write(csv_content.encode(self._encoding))
            self._written_files.append(file_url)
        except Exception as e:
            raise ConnectorError(
                f"Failed to write file: {e}",
                context={"path": file_url, "backend": self._backend},
            ) from e

    @property
    def written_files(self) -> list[str]:
        """Return list of files written during this session."""
        return self._written_files.copy()


@register_connector("filestore")
def create_filestore_connector(
    config: ConnectorConfigUnion, connection: dict[str, Any] | None = None
) -> FileStoreConnector:
    """Factory function for creating FileStoreConnector instances."""
    return FileStoreConnector(config, connection)

