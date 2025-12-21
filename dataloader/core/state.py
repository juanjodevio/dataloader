"""State management for incremental data loading."""

from copy import deepcopy
from typing import Any

from pydantic import BaseModel, Field


class State(BaseModel):
    """Immutable state snapshot for incremental data loading.

    State tracks cursor values, watermarks, checkpoints, and metadata
    to enable resumable and incremental data loads.
    """

    cursor_values: dict[str, Any] = Field(default_factory=dict)
    watermarks: dict[str, Any] = Field(default_factory=dict)
    checkpoints: list[dict[str, Any]] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)

    def update(
        self,
        cursor_values: dict[str, Any] | None = None,
        watermarks: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> "State":
        """Create a new State instance with updated values.

        This method returns a new State instance, preserving immutability.
        The original state remains unchanged.

        Args:
            cursor_values: New cursor values to merge (shallow merge)
            watermarks: New watermark values to merge (shallow merge)
            metadata: New metadata to merge (shallow merge)

        Returns:
            New State instance with updated values
        """
        new_cursor_values = {**self.cursor_values}
        if cursor_values:
            new_cursor_values.update(cursor_values)

        new_watermarks = {**self.watermarks}
        if watermarks:
            new_watermarks.update(watermarks)

        new_metadata = {**self.metadata}
        if metadata:
            new_metadata.update(metadata)

        return State(
            cursor_values=new_cursor_values,
            watermarks=new_watermarks,
            checkpoints=deepcopy(self.checkpoints),
            metadata=new_metadata,
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert state to dictionary representation.

        Returns:
            Dictionary representation of the state
        """
        return self.model_dump()

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "State":
        """Create State instance from dictionary.

        Args:
            data: Dictionary containing state data

        Returns:
            State instance
        """
        return cls(**data)
