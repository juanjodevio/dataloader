"""Runtime configuration model for recipe definitions."""

from pydantic import BaseModel, Field, field_validator


class RuntimeConfig(BaseModel):
    """Configuration for runtime behavior."""

    batch_size: int = Field(
        default=10000, description="Number of rows per batch", gt=0
    )
    max_retries: int = Field(
        default=0, description="Maximum retry attempts (stub for v0.2)", ge=0
    )

    @field_validator("batch_size")
    @classmethod
    def validate_batch_size(cls, v):
        """Validate batch_size is positive."""
        if v <= 0:
            raise ValueError("batch_size must be greater than 0")
        return v

