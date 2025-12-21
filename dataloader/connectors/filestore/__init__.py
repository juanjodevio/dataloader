"""FileStore connector module."""

from dataloader.connectors.filestore.config import (
    FileStoreConfigType,
    FileStoreConnectorConfig,
    LocalFileStoreConfig,
    S3FileStoreConfig,
)
from dataloader.connectors.filestore.connector import (
    FileStoreConnector,
    create_filestore_connector,
)
from dataloader.connectors.filestore.formats import (
    CSVFormat,
    Format,
    JSONFormat,
    JSONLFormat,
    ParquetFormat,
    get_format,
    list_formats,
    register_format,
)

__all__ = [
    "FileStoreConnector",
    "FileStoreConnectorConfig",
    "FileStoreConfigType",
    "S3FileStoreConfig",
    "LocalFileStoreConfig",
    "create_filestore_connector",
    # Format handlers
    "Format",
    "CSVFormat",
    "JSONFormat",
    "JSONLFormat",
    "ParquetFormat",
    "get_format",
    "list_formats",
    "register_format",
]
