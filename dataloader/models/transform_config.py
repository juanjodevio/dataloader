"""Transform configuration model for recipe definitions."""

from typing import List

from pydantic import BaseModel, ConfigDict, Field


class TransformStep(BaseModel):
    """
    A single transform step in the pipeline.

    Allows flexible transform-specific fields beyond 'type'.
    """

    model_config = ConfigDict(extra="allow")

    type: str = Field(description="Transform type (e.g., 'rename_columns', 'cast', 'add_column')")


class TransformConfig(BaseModel):
    """Configuration for transform pipeline."""

    steps: List[TransformStep] = Field(
        default_factory=list, description="List of transform steps to apply"
    )

