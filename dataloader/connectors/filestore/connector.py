"""Unified FileStore connector for reading and writing files across multiple storage backends.

Uses fsspec for abstraction, supporting S3, local filesystem, Azure, GCS, and other backends.
"""

from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Union

import fsspec
from fsspec import AbstractFileSystem

from dataloader.connectors.registry import ConnectorConfigUnion, register_connector
from dataloader.core.batch import ArrowBatch, Batch
from dataloader.core.exceptions import ConnectorError
from dataloader.core.state import State
from dataloader.models.destination_config import DestinationConfig
from dataloader.models.source_config import SourceConfig

from .config import FileStoreConfigType, LocalFileStoreConfig, S3FileStoreConfig
from .formats import Format, get_format

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
    ):
        """Initialize FileStoreConnector.

        Args:
            config: FileStore connector configuration (FileStoreConfigType, SourceConfig, or DestinationConfig).
                All configuration, including connection parameters, should be in the config parameter.
        """
        self._config = config

        # Extract config values
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

        # Reading configuration (using defaults)
        self._batch_size = DEFAULT_BATCH_SIZE
        self._encoding = DEFAULT_ENCODING

        # Format-specific options (for CSV, using defaults)
        format_options = {
            "delimiter": ",",
            "has_header": True,
        }

        # Initialize format handler
        self._format_handler = get_format(self._format, **format_options)

        # Writing state
        self._file_initialized = False
        self._batch_counter = 0
        self._written_files: list[str] = []

        # Build fsspec storage options
        self._storage_options = self._build_storage_options(config, None)
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

    def _list_files(self) -> list[dict[str, Any]]:
        """List files in the configured path using fsspec."""
        fs = self._get_filesystem()
        file_url = self._build_file_url(self._path)

        try:
            # For directories, list all files
            if fs.isdir(file_url):
                files = []
                # Get valid extensions for this format
                valid_extensions = self._format_handler.extensions
                for file_path in fs.find(file_url):
                    # Filter by format extension
                    if any(file_path.lower().endswith(ext.lower()) for ext in valid_extensions):
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
            # Ensure file_path is a string (not a Path object) for fsspec
            file_path_str = str(file_path)

            # Filter by modification time
            if last_modified_cursor:
                try:
                    mod_time = fs.modified(file_path_str)
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
            if last_file_cursor and file_path_str <= str(last_file_cursor):
                continue

            filtered.append(file_info)

        return filtered


    def read_batches(self, state: State) -> Iterable[DictBatch]:
        """Read files from FileStore as batches.

        Uses fsspec for unified file operations across all backends and format handlers
        for reading different file formats.

        Args:
            state: Current state containing cursor values for incremental reads.

        Yields:
            DictBatch instances containing the data.

        Raises:
            NotImplementedError: If reading is not supported (should not happen for FileStore).
            ConnectorError: If file operations fail.
        """
        try:
            # List files
            files = self._list_files()
            files = self._filter_files_by_state(files, state)

            # Sort by path for consistent processing order
            files.sort(key=lambda x: x["path"])

            # Read each file using format handler
            fs = self._get_filesystem()
            for file_info in files:
                file_path = file_info["path"]
                # Ensure file_path is a string (not a Path object) for fsspec
                file_path_str = str(file_path)

                try:
                    # Read file using fsspec
                    # For binary formats (parquet), read as bytes; for text formats, read as string
                    if self._format == "parquet":
                        with fs.open(file_path_str, mode="rb") as f:
                            content = f.read()
                    else:
                        with fs.open(file_path_str, mode="r", encoding=self._encoding) as f:
                            content = f.read()

                    # Use format handler to read batches
                    yield from self._format_handler.read_batches(
                        content,
                        file_path_str,
                        batch_size=self._batch_size,
                        encoding=self._encoding,
                    )
                except Exception as e:
                    raise ConnectorError(
                        f"Failed to read file: {e}",
                        context={"path": file_path_str, "backend": self._backend, "format": self._format},
                    ) from e

        except ConnectorError:
            raise
        except Exception as e:
            raise ConnectorError(
                f"Unexpected error reading from FileStore: {e}",
                context={"path": self._path, "backend": self._backend, "format": self._format},
            ) from e

    # ========== Writing methods ==========

    def _generate_file_path(self, batch: Batch) -> str:
        """Generate file path for the batch."""
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        self._batch_counter += 1

        # Get extension from format handler (use first extension)
        ext = self._format_handler.extensions[0] if self._format_handler.extensions else ".dat"

        filename = f"data_{timestamp}_{self._batch_counter:04d}{ext}"

        # Build full path
        base_path = self._path.rstrip("/")
        if base_path:
            return f"{base_path}/{filename}"
        return filename

    def _delete_existing_files(self) -> None:
        """Delete existing files at the destination path (for overwrite mode)."""
        fs = self._get_filesystem()
        file_url = self._build_file_url(self._path)

        try:
            if fs.isdir(file_url):
                # List and delete all files matching the format extensions
                valid_extensions = self._format_handler.extensions
                for file_path in fs.find(file_url):
                    if any(file_path.lower().endswith(ext.lower()) for ext in valid_extensions):
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
        """Write a batch to FileStore using the configured format handler.

        Args:
            batch: Batch of data to write.
            state: Current pipeline state.

        Raises:
            NotImplementedError: If writing is not supported (should not happen for FileStore).
            ConnectorError: If file operations fail.
        """
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

        # Generate file path and convert to format using format handler
        file_path = self._generate_file_path(batch)
        file_url = self._build_file_url(file_path)
        file_content = self._format_handler.write_batch(batch, encoding=self._encoding)

        # Write using fsspec (always binary mode since format handler returns bytes)
        fs = self._get_filesystem()
        try:
            with fs.open(file_url, mode="wb") as f:
                f.write(file_content)
            self._written_files.append(file_url)
        except Exception as e:
            raise ConnectorError(
                f"Failed to write file: {e}",
                context={"path": file_url, "backend": self._backend, "format": self._format},
            ) from e

    @property
    def written_files(self) -> list[str]:
        """Return list of files written during this session."""
        return self._written_files.copy()


@register_connector("filestore")
def create_filestore_connector(
    config: ConnectorConfigUnion,
) -> FileStoreConnector:
    """Factory function for creating FileStoreConnector instances."""
    return FileStoreConnector(config)

