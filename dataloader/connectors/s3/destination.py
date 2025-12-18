"""S3 destination connector for writing CSV files to S3."""

import csv
from datetime import datetime
from io import StringIO
from typing import Any

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from dataloader.connectors.registry import register_destination
from dataloader.core.batch import Batch
from dataloader.core.exceptions import ConnectorError
from dataloader.core.state import State
from dataloader.models.destination_config import DestinationConfig


class S3Destination:
    """Destination connector for writing data to S3 as CSV files.

    Supports append and overwrite write modes with optional partitioning.
    For v0.1, only CSV format is supported.
    """

    DEFAULT_ENCODING = "utf-8"

    def __init__(self, config: DestinationConfig, connection: dict[str, Any]):
        """Initialize S3 destination.

        Args:
            config: Destination configuration with bucket and path.
            connection: Connection parameters. Supports:
                - aws_access_key_id: AWS access key
                - aws_secret_access_key: AWS secret key
                - region_name: AWS region
                - endpoint_url: Custom S3 endpoint (for LocalStack/MinIO)
                - partition_key: Column name for partitioning
                - file_prefix: Prefix for file names (default: 'data')
        """
        self._config = config
        self._connection = connection
        self._bucket = connection.get("bucket") or config.bucket
        self._base_path = (connection.get("path") or config.path or "").strip("/")
        self._write_mode = config.write_mode
        self._encoding = connection.get("encoding", self.DEFAULT_ENCODING)
        self._partition_key = connection.get("partition_key")
        self._file_prefix = connection.get("file_prefix", "data")

        # Build client configuration
        self._boto_config = self._build_boto_config(config, connection)
        self._s3_client: Any = None
        self._batch_counter = 0
        self._written_files: list[str] = []

    def _build_boto_config(
        self, config: DestinationConfig, connection: dict[str, Any]
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

        # Custom endpoint for LocalStack/MinIO
        if "endpoint_url" in connection:
            client_config["endpoint_url"] = connection["endpoint_url"]

        return client_config

    def _get_client(self) -> Any:
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
        client = self._get_client()

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
                    client.delete_objects(
                        Bucket=self._bucket, Delete={"Objects": batch}
                    )

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
        client = self._get_client()

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


@register_destination("s3")
def create_s3_destination(
    config: DestinationConfig, connection: dict[str, Any]
) -> S3Destination:
    """Factory function for creating S3Destination instances."""
    return S3Destination(config, connection)

