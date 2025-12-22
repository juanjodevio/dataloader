"""Data Loader - Recipe-driven data loading framework.

A declarative data loading engine built around YAML recipes, enabling
robust EL workflows with minimal code.
"""

__version__ = "0.0.0b5"

# Public API
from dataloader.api import from_yaml, run_recipe, run_recipe_from_yaml

# Core classes
from dataloader.core.batch import ArrowBatch, Batch

# Exceptions
from dataloader.core.exceptions import (
    ConnectorError,
    DataLoaderError,
    EngineError,
    RecipeError,
    StateError,
    TransformError,
)
from dataloader.core.state import State
from dataloader.core.state_backend import LocalStateBackend, StateBackend

# Recipe model
from dataloader.models.recipe import Recipe

__all__ = [
    # Version
    "__version__",
    # Public API
    "from_yaml",
    "run_recipe",
    "run_recipe_from_yaml",
    # Core classes
    "Recipe",
    "State",
    "StateBackend",
    "LocalStateBackend",
    "Batch",
    "ArrowBatch",
    # Exceptions
    "DataLoaderError",
    "RecipeError",
    "ConnectorError",
    "TransformError",
    "StateError",
    "EngineError",
]
