"""S3 connector module."""

from dataloader.connectors.s3.config import S3ConnectorConfig
from dataloader.connectors.s3.connector import S3Connector, create_s3_connector
from dataloader.connectors.s3.destination import S3Destination, create_s3_destination
from dataloader.connectors.s3.source import S3Source, create_s3_source

__all__ = [
    "S3Connector",
    "S3ConnectorConfig",
    "create_s3_connector",
    # Legacy exports for backward compatibility
    "S3Source",
    "S3Destination",
    "create_s3_source",
    "create_s3_destination",
]

