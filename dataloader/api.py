"""Public Python API for dataloader package.

This module provides the main entry points for loading and executing recipes.
"""

from dataloader.core.engine import execute
from dataloader.core.state_backend import LocalStateBackend, StateBackend
from dataloader.models.loader import load_recipe
from dataloader.models.recipe import Recipe


def from_yaml(path: str) -> Recipe:
    """Load recipe from YAML file with inheritance resolution.

    This function loads a recipe from a YAML file, resolving any inheritance
    via the `extends` field, and applying template rendering.

    Args:
        path: Path to recipe YAML file

    Returns:
        Fully resolved Recipe instance

    Raises:
        RecipeError: If file not found, invalid YAML, validation fails, or cycle detected

    Example:
        >>> recipe = from_yaml("examples/recipes/customers.yaml")
        >>> print(recipe.name)
        customers
    """
    return load_recipe(path, cli_vars=None)


def run_recipe(
    recipe: Recipe,
    state_backend: StateBackend,
) -> None:
    """Execute a recipe with given state backend.

    This function executes a complete data loading pipeline:
    1. Loads state for the recipe
    2. Reads batches from source
    3. Applies transforms to each batch
    4. Writes batches to destination
    5. Saves state after each batch

    All connection parameters come from the recipe configuration, which supports
    Jinja2-style templates (e.g., {{ env_var('DB_HOST') }}) that are rendered
    during recipe loading via `from_yaml()`.

    Args:
        recipe: Recipe instance to execute. Source and destination configuration
                come from recipe.source and recipe.destination. Connection parameters
                (host, user, password, etc.) are specified in the recipe and can
                use templates like {{ env_var('DB_HOST') }}.
        state_backend: Backend for loading and saving state

    Raises:
        EngineError: If execution fails at any step
        ConnectorError: If connector creation fails
        TransformError: If transform execution fails
        StateError: If state operations fail

    Example:
        >>> from dataloader import from_yaml, LocalStateBackend
        >>> recipe = from_yaml("examples/recipes/customers.yaml")
        >>> state_backend = LocalStateBackend(".state")
        >>> run_recipe(recipe, state_backend)
    """
    execute(recipe, state_backend)


def run_recipe_from_yaml(
    recipe_path: str,
    state_dir: str = ".state",
) -> None:
    """Load and execute recipe from YAML file.

    Convenience function that combines `from_yaml()` and `run_recipe()`.
    Creates a LocalStateBackend with the specified state directory.

    Connection parameters are specified in the recipe YAML file using Jinja2-style
    templates (e.g., {{ env_var('DB_HOST') }}) and are rendered during loading.

    Args:
        recipe_path: Path to recipe YAML file
        state_dir: Directory to store state files (default: ".state")

    Raises:
        RecipeError: If recipe loading fails
        EngineError: If execution fails
        ConnectorError: If connector creation fails
        TransformError: If transform execution fails
        StateError: If state operations fail

    Example:
        >>> from dataloader import run_recipe_from_yaml
        >>> run_recipe_from_yaml("examples/recipes/customers.yaml", state_dir=".state")
    """
    recipe = from_yaml(recipe_path)
    state_backend = LocalStateBackend(state_dir)
    run_recipe(recipe, state_backend)

