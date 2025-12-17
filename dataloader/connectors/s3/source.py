"""S3 source connector using boto3 for discovery and fsspec for reads."""

import csv
from datetime import datetime
from io import StringIO
from typing import Any, Iterable

import boto3
import fsspec
from botocore.exceptions import BotoCoreError, ClientError

from dataloader.core.batch import DictBatch
from dataloader.core.exceptions import ConnectorError
from dataloader.core.state import State
from dataloader.models.source_config import SourceConfig


class S3Source:
    """Source connector for S3 buckets.

    Uses boto3 for object discovery and metadata (fast, explicit, cheap),
    and fsspec for reading file contents (clean file-like interface).
    For v0.1, only CSV files are supported.
    """

    DEFAULT_BATCH_SIZE = 1000
    DEFAULT_ENCODING = "utf-8"

    def __init__(self, config: SourceConfig, connection: dict[str, Any]):
        """Initialize S3Source.

        Args:
            config: Source configuration containing bucket, path, and incremental settings.
            connection: Connection parameters (aws_access_key_id, aws_secret_access_key, region_name).
        """
        self._config = config
        self._connection = connection
        self._batch_size = connection.get("batch_size", self.DEFAULT_BATCH_SIZE)
        self._encoding = connection.get("encoding", self.DEFAULT_ENCODING)
        self._delimiter = connection.get("delimiter", ",")
        self._has_header = connection.get("has_header", True)

        # Build client configurations
        self._boto_config = self._build_boto_config(config, connection)
        self._fsspec_config = self._build_fsspec_config(config, connection)
        self._s3_client: Any = None

    def _build_boto_config(
        self, config: SourceConfig, connection: dict[str, Any]
    ) -> dict[str, Any]:
        """Build boto3 client configuration."""
        client_config: dict[str, Any] = {}

        access_key = connection.get("aws_access_key_id") or config.access_key
        secret_key = connection.get("aws_secret_access_key") or config.secret_key
        region = connection.get("region_name") or config.region

        if access_key and secret_key:
            client_config["aws_access_key_id"] = access_key
            client_config["aws_secret_access_key"] = secret_key

        if region:
            client_config["region_name"] = region

        # Support custom endpoint (LocalStack, MinIO)
        if "endpoint_url" in connection:
            client_config["endpoint_url"] = connection["endpoint_url"]

        return client_config

    def _build_fsspec_config(
        self, config: SourceConfig, connection: dict[str, Any]
    ) -> dict[str, Any]:
        """Build fsspec storage options for s3fs."""
        storage_options: dict[str, Any] = {}

        access_key = connection.get("aws_access_key_id") or config.access_key
        secret_key = connection.get("aws_secret_access_key") or config.secret_key
        region = connection.get("region_name") or config.region

        if access_key and secret_key:
            storage_options["key"] = access_key
            storage_options["secret"] = secret_key

        if region:
            storage_options["client_kwargs"] = {"region_name": region}

        # Support custom endpoint (LocalStack, MinIO)
        if "endpoint_url" in connection:
            if "client_kwargs" not in storage_options:
                storage_options["client_kwargs"] = {}
            storage_options["client_kwargs"]["endpoint_url"] = connection["endpoint_url"]

        return storage_options

    def _get_boto_client(self) -> Any:
        """Get or create boto3 S3 client for discovery."""
        if self._s3_client is None:
            try:
                self._s3_client = boto3.client("s3", **self._boto_config)
            except (BotoCoreError, ClientError) as e:
                raise ConnectorError(
                    f"Failed to create S3 client: {e}",
                    context={"region": self._boto_config.get("region_name")},
                ) from e
        return self._s3_client

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
            with fsspec.open(s3_path, mode="r", encoding=self._encoding, **self._fsspec_config) as f:
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

    def _infer_schema(self, columns: list[str], sample_rows: list[list[str]]) -> dict[str, str]:
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
                    "s3_bucket": self._config.bucket,
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
        bucket = self._config.bucket
        prefix = self._config.path or ""

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


def create_s3_source(config: SourceConfig, connection: dict[str, Any]) -> S3Source:
    """Factory function for creating S3Source instances."""
    return S3Source(config, connection)
