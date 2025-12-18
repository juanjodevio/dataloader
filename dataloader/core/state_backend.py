"""State backend protocol and implementations for persisting state."""

import json
from pathlib import Path
from typing import Any, Protocol

from dataloader.core.exceptions import StateError


class StateBackend(Protocol):
    """Protocol for state persistence backends."""

    def load(self, recipe_name: str) -> dict[str, Any]:
        """Load state for a recipe.

        Args:
            recipe_name: Name of the recipe

        Returns:
            Dictionary representation of the state

        Raises:
            StateError: If loading fails
        """
        ...

    def save(self, recipe_name: str, state: dict[str, Any]) -> None:
        """Save state for a recipe.

        Args:
            recipe_name: Name of the recipe
            state: Dictionary representation of the state

        Raises:
            StateError: If saving fails
        """
        ...


class LocalStateBackend:
    """Local file-based state backend.

    Stores state in JSON files under `.state/{recipe_name}.json`.
    Uses atomic writes (write to temp file, then rename) to prevent corruption.
    """

    def __init__(self, state_dir: str | Path = ".state"):
        """Initialize local state backend.

        Args:
            state_dir: Directory to store state files (default: `.state`)
        """
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)

    def load(self, recipe_name: str) -> dict[str, Any]:
        """Load state for a recipe from local JSON file.

        Args:
            recipe_name: Name of the recipe

        Returns:
            Dictionary representation of the state (empty dict if file doesn't exist)

        Raises:
            StateError: If file exists but cannot be read or parsed
        """
        state_file = self.state_dir / f"{recipe_name}.json"

        if not state_file.exists():
            return {}

        try:
            with open(state_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data if isinstance(data, dict) else {}
        except json.JSONDecodeError as e:
            raise StateError(
                f"Failed to parse state file {state_file}: {e}",
                context={"recipe_name": recipe_name, "state_file": str(state_file)},
            ) from e
        except OSError as e:
            raise StateError(
                f"Failed to read state file {state_file}: {e}",
                context={"recipe_name": recipe_name, "state_file": str(state_file)},
            ) from e

    def save(self, recipe_name: str, state: dict[str, Any]) -> None:
        """Save state for a recipe to local JSON file using atomic write.

        Args:
            recipe_name: Name of the recipe
            state: Dictionary representation of the state

        Raises:
            StateError: If saving fails
        """
        state_file = self.state_dir / f"{recipe_name}.json"
        temp_file = self.state_dir / f"{recipe_name}.json.tmp"

        try:
            with open(temp_file, "w", encoding="utf-8") as f:
                json.dump(state, f, indent=2, ensure_ascii=False)

            temp_file.replace(state_file)
        except OSError as e:
            if temp_file.exists():
                temp_file.unlink()
            raise StateError(
                f"Failed to save state file {state_file}: {e}",
                context={"recipe_name": recipe_name, "state_file": str(state_file)},
            ) from e
        except (TypeError, ValueError) as e:
            if temp_file.exists():
                temp_file.unlink()
            raise StateError(
                f"Failed to serialize state for {recipe_name}: {e}",
                context={"recipe_name": recipe_name},
            ) from e

