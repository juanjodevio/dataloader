"""Runtime configuration model for recipe definitions."""

from pydantic import BaseModel, Field, field_validator


class RuntimeConfig(BaseModel):
    """Configuration for runtime behavior."""

    batch_size: int = Field(default=10000, description="Number of rows per batch", gt=0)
    parallelism: int = Field(
        default=1,
        description="Number of parallel workers for batch processing (1 = sequential)",
        ge=1,
    )
    full_refresh: bool = Field(
        default=False,
        description="Enable full refresh (destructive: drops/recreates tables or deletes paths). Use with caution as this will delete existing data and structures.",
    )

    @field_validator("batch_size")
    @classmethod
    def validate_batch_size(cls, v):
        """Validate batch_size is positive."""
        if v <= 0:
            raise ValueError("batch_size must be greater than 0")
        return v

    @field_validator("parallelism")
    @classmethod
    def validate_parallelism(cls, v):
        """Validate parallelism is at least 1."""
        if v < 1:
            raise ValueError("parallelism must be at least 1")
        return v
