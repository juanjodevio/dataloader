"""CSV connector configuration."""

from typing import Literal, Optional

from pydantic import BaseModel, Field

from dataloader.models.source_config import IncrementalConfig


class CSVConnectorConfig(BaseModel):
    """Configuration for CSV connector.

    Supports both reading and writing operations.
    """

    type: Literal["csv"] = "csv"

    # File path
    path: str = Field(description="CSV file path (supports templates)")

    # Source-specific fields (for reading)
    incremental: Optional[IncrementalConfig] = Field(
        default=None, description="Incremental loading configuration (for reads)"
    )

    # Destination-specific fields (for writing)
    write_mode: Literal["append", "overwrite"] = Field(
        default="append", description="Write mode for destination (for writes)"
    )

