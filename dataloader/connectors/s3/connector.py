"""S3 connector for reading and writing CSV files to/from S3."""

import csv
from datetime import datetime
from io import StringIO
from typing import Any, Iterable, Union

import boto3
import fsspec
from botocore.exceptions import BotoCoreError, ClientError

from dataloader.connectors.registry import ConnectorConfigUnion, register_connector
from dataloader.core.batch import Batch, DictBatch
from dataloader.core.exceptions import ConnectorError
from dataloader.core.state import State
from dataloader.models.destination_config import DestinationConfig
from dataloader.models.source_config import SourceConfig

from .config import S3ConnectorConfig


class S3Connector:
    """Unified connector for S3 buckets supporting both read and write operations.

    Uses boto3 for object discovery and metadata (fast, explicit, cheap),
    and fsspec for reading file contents (clean file-like interface).
    For v0.1, only CSV files are supported.
    """

    DEFAULT_BATCH_SIZE = 1000
    DEFAULT_ENCODING = "utf-8"

    def __init__(
        self,
        config: Union[S3ConnectorConfig, SourceConfig, DestinationConfig],
        connection: dict[str, Any] | None = None,
    ):
        """Initialize S3Connector.

        Args:
            config: S3 connector configuration (S3ConnectorConfig, SourceConfig, or DestinationConfig).
            connection: Optional connection parameters (legacy, defaults to empty dict).
                All configuration should be in the config parameter. This parameter is kept
                for backward compatibility with the factory signature.
        """
        self._config = config
        self._connection = connection or {}

        # Extract config values (support both new and legacy configs)
        if isinstance(config, S3ConnectorConfig):
            self._bucket = config.bucket
            self._base_path = config.path.strip("/") if config.path else ""
            self._write_mode = getattr(config, "write_mode", "append")
        elif isinstance(config, SourceConfig):
            self._bucket = config.bucket or ""
            self._base_path = (config.path or "").strip("/")
            self._write_mode = "append"  # Default for source configs
        else:  # DestinationConfig
            self._bucket = config.bucket or ""
            self._base_path = (config.path or "").strip("/")
            self._write_mode = config.write_mode

        # Reading configuration
        self._batch_size = connection.get("batch_size", self.DEFAULT_BATCH_SIZE)
        self._encoding = connection.get("encoding", self.DEFAULT_ENCODING)
        self._delimiter = connection.get("delimiter", ",")
        self._has_header = connection.get("has_header", True)

        # Writing configuration
        self._partition_key = connection.get("partition_key")
        self._file_prefix = connection.get("file_prefix", "data")
        self._batch_counter = 0
        self._written_files: list[str] = []

        # Build client configurations
        self._boto_config = self._build_boto_config(config, self._connection)
        self._fsspec_config = self._build_fsspec_config(config, self._connection)
        self._s3_client: Any = None

    def _build_boto_config(
        self,
        config: Union[S3ConnectorConfig, SourceConfig, DestinationConfig],
        connection: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Build boto3 client configuration."""
        client_config: dict[str, Any] = {}

        # Extract credentials from config or connection
        conn = connection or {}
        access_key = conn.get("aws_access_key_id") or getattr(config, "access_key", None)
        secret_key = conn.get("aws_secret_access_key") or getattr(config, "secret_key", None)
        region = conn.get("region_name") or getattr(config, "region", None)

        if access_key and secret_key:
            client_config["aws_access_key_id"] = access_key
            client_config["aws_secret_access_key"] = secret_key

        if region:
            client_config["region_name"] = region

        # Support custom endpoint (LocalStack, MinIO)
        if conn.get("endpoint_url"):
            client_config["endpoint_url"] = conn["endpoint_url"]

        return client_config

    def _build_fsspec_config(
        self,
        config: Union[S3ConnectorConfig, SourceConfig, DestinationConfig],
        connection: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Build fsspec storage options for s3fs."""
        storage_options: dict[str, Any] = {}

        conn = connection or {}
        access_key = conn.get("aws_access_key_id") or getattr(config, "access_key", None)
        secret_key = conn.get("aws_secret_access_key") or getattr(config, "secret_key", None)
        region = conn.get("region_name") or getattr(config, "region", None)

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

        return storage_options

    def _get_boto_client(self) -> Any:
        """Get or create boto3 S3 client."""
        if self._s3_client is None:
            try:
                self._s3_client = boto3.client("s3", **self._boto_config)
            except (BotoCoreError, ClientError) as e:
                raise ConnectorError(
                    f"Failed to create S3 client: {e}",
                    context={"region": self._boto_config.get("region_name")},
                ) from e
        return self._s3_client

    # ========== Reading methods (from S3Source) ==========

    def _list_objects(self, bucket: str, prefix: str) -> list[dict[str, Any]]:
        """List objects in S3 bucket with given prefix using boto3.

        Args:
            bucket: S3 bucket name.
            prefix: Object key prefix (path).

        Returns:
            List of object metadata dicts with 'Key', 'LastModified', 'Size'.
        """
        client = self._get_boto_client()
        objects = []

        try:
            paginator = client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    # Only include CSV files for v0.1
                    if obj["Key"].lower().endswith(".csv"):
                        objects.append(obj)
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            raise ConnectorError(
                f"Failed to list S3 objects: {e}",
                context={
                    "bucket": bucket,
                    "prefix": prefix,
                    "error_code": error_code,
                },
            ) from e

        return objects

    def _filter_objects_by_state(
        self, objects: list[dict[str, Any]], state: State
    ) -> list[dict[str, Any]]:
        """Filter objects based on incremental state.

        Args:
            objects: List of S3 object metadata.
            state: Current state containing cursor values.

        Returns:
            Filtered list of objects to process.
        """
        last_modified_cursor = state.cursor_values.get("last_modified")
        last_file_cursor = state.cursor_values.get("last_file")

        if not last_modified_cursor and not last_file_cursor:
            return objects

        filtered = []
        for obj in objects:
            # Filter by modification time
            if last_modified_cursor:
                obj_modified = obj["LastModified"]
                if isinstance(obj_modified, datetime):
                    obj_modified_str = obj_modified.isoformat()
                else:
                    obj_modified_str = str(obj_modified)

                if obj_modified_str <= str(last_modified_cursor):
                    continue

            # Filter by file name (lexicographic order)
            if last_file_cursor and obj["Key"] <= str(last_file_cursor):
                continue

            filtered.append(obj)

        return filtered

    def _read_file_with_fsspec(self, bucket: str, key: str) -> str:
        """Read S3 object contents using fsspec.

        Args:
            bucket: S3 bucket name.
            key: Object key.

        Returns:
            File contents as string.
        """
        s3_path = f"s3://{bucket}/{key}"

        try:
            with fsspec.open(
                s3_path, mode="r", encoding=self._encoding, **self._fsspec_config
            ) as f:
                return f.read()
        except Exception as e:
            raise ConnectorError(
                f"Failed to read S3 object with fsspec: {e}",
                context={"bucket": bucket, "key": key, "path": s3_path},
            ) from e

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

    def _read_csv_batches(
        self,
        content: str,
        s3_key: str,
        s3_metadata: dict[str, Any],
    ) -> Iterable[DictBatch]:
        """Read batches from CSV content string.

        Args:
            content: CSV file contents as string.
            s3_key: Original S3 key for metadata.
            s3_metadata: S3 object metadata.

        Yields:
            DictBatch instances.
        """
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

        column_types: dict[str, str] = {}
        batch_number = 0

        # Read all rows into buffer first (since content is already in memory)
        for row in reader:
            rows_buffer.append(row)

        # Infer schema from first 100 rows
        schema_sample = rows_buffer[: min(100, len(rows_buffer))]
        column_types = self._infer_schema(columns, schema_sample)

        # Yield in batches
        for i in range(0, len(rows_buffer), self._batch_size):
            batch_rows = rows_buffer[i : i + self._batch_size]
            batch_number += 1

            # Prepare last_modified for metadata
            last_modified = s3_metadata.get("LastModified")
            if isinstance(last_modified, datetime):
                last_modified = last_modified.isoformat()

            yield DictBatch(
                columns=columns,
                rows=batch_rows,
                metadata={
                    "batch_number": batch_number,
                    "row_count": len(batch_rows),
                    "source_type": "s3",
                    "s3_key": s3_key,
                    "s3_bucket": self._bucket,
                    "s3_last_modified": last_modified,
                    "column_types": column_types,
                },
            )

    def read_batches(self, state: State) -> Iterable[DictBatch]:
        """Read CSV files from S3 bucket as batches.

        Uses boto3 for object discovery and metadata, and fsspec for
        reading file contents.

        Args:
            state: Current state containing cursor values for incremental reads.

        Yields:
            DictBatch instances containing the data.

        Raises:
            ConnectorError: If S3 operations fail.
        """
        bucket = self._bucket
        prefix = self._base_path or ""

        # Normalize prefix (remove leading slash)
        if prefix.startswith("/"):
            prefix = prefix[1:]

        try:
            # List and filter objects using boto3
            objects = self._list_objects(bucket, prefix)
            objects = self._filter_objects_by_state(objects, state)

            # Sort by LastModified for consistent processing order
            objects.sort(key=lambda x: (x.get("LastModified", ""), x.get("Key", "")))

            for obj in objects:
                s3_key = obj["Key"]

                # Read file using fsspec
                content = self._read_file_with_fsspec(bucket, s3_key)
                yield from self._read_csv_batches(content, s3_key, obj)

        except ConnectorError:
            raise
        except Exception as e:
            raise ConnectorError(
                f"Unexpected error reading from S3: {e}",
                context={"bucket": bucket, "prefix": prefix},
            ) from e

    # ========== Writing methods (from S3Destination) ==========

    def _generate_key(self, batch: Batch) -> str:
        """Generate S3 key for the batch.

        Supports partitioning by column value if partition_key is configured.
        File names include timestamp and batch counter for uniqueness.
        """
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        self._batch_counter += 1
        filename = f"{self._file_prefix}_{timestamp}_{self._batch_counter:04d}.csv"

        # Build path parts
        path_parts = []
        if self._base_path:
            path_parts.append(self._base_path)

        # Add partition if configured
        if self._partition_key and self._partition_key in batch.columns:
            col_idx = batch.columns.index(self._partition_key)
            if batch.rows:
                partition_value = batch.rows[0][col_idx]
                path_parts.append(f"{self._partition_key}={partition_value}")

        path_parts.append(filename)
        return "/".join(path_parts)

    def _batch_to_csv(self, batch: Batch) -> str:
        """Convert batch to CSV string."""
        output = StringIO()
        writer = csv.writer(output)
        writer.writerow(batch.columns)
        writer.writerows(batch.rows)
        return output.getvalue()

    def _delete_existing_files(self) -> None:
        """Delete existing files at the destination path (for overwrite mode)."""
        client = self._get_boto_client()

        try:
            paginator = client.get_paginator("list_objects_v2")
            prefix = f"{self._base_path}/" if self._base_path else ""

            objects_to_delete = []
            for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    if obj["Key"].endswith(".csv"):
                        objects_to_delete.append({"Key": obj["Key"]})

            if objects_to_delete:
                # Delete in batches of 1000 (S3 limit)
                for i in range(0, len(objects_to_delete), 1000):
                    batch = objects_to_delete[i : i + 1000]
                    client.delete_objects(Bucket=self._bucket, Delete={"Objects": batch})

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            raise ConnectorError(
                f"Failed to delete existing S3 files: {e}",
                context={
                    "bucket": self._bucket,
                    "prefix": self._base_path,
                    "error_code": error_code,
                },
            ) from e

    def _upload_csv(self, key: str, csv_content: str) -> None:
        """Upload CSV content to S3."""
        client = self._get_boto_client()

        try:
            client.put_object(
                Bucket=self._bucket,
                Key=key,
                Body=csv_content.encode(self._encoding),
                ContentType="text/csv",
            )
            self._written_files.append(key)
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            raise ConnectorError(
                f"Failed to upload to S3: {e}",
                context={
                    "bucket": self._bucket,
                    "key": key,
                    "error_code": error_code,
                },
            ) from e

    def write_batch(self, batch: Batch, state: State) -> None:
        """Write a batch to S3 as a CSV file.

        Args:
            batch: Batch of data to write.
            state: Current pipeline state. Updates with written file info.

        Raises:
            ConnectorError: If write mode is unsupported or S3 operations fail.
        """
        if self._write_mode == "merge":
            raise ConnectorError(
                "Merge write mode is not supported for S3 in v0.1. Use 'append' or 'overwrite'.",
                context={"bucket": self._bucket, "write_mode": self._write_mode},
            )

        # Handle overwrite on first batch
        if self._write_mode == "overwrite" and self._batch_counter == 0:
            self._delete_existing_files()

        if not batch.rows:
            return

        # Generate key and convert to CSV
        key = self._generate_key(batch)
        csv_content = self._batch_to_csv(batch)

        # Upload to S3
        self._upload_csv(key, csv_content)

    @property
    def written_files(self) -> list[str]:
        """Return list of files written during this session."""
        return self._written_files.copy()


@register_connector("s3")
def create_s3_connector(
    config: ConnectorConfigUnion, connection: dict[str, Any]
) -> S3Connector:
    """Factory function for creating S3Connector instances."""
    return S3Connector(config, connection)

