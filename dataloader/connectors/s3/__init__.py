"""S3 connector module."""

from dataloader.connectors.s3.destination import S3Destination, create_s3_destination
from dataloader.connectors.s3.source import S3Source, create_s3_source

__all__ = [
    "S3Source",
    "S3Destination",
    "create_s3_source",
    "create_s3_destination",
]

