"""Recipe loader with YAML parsing and inheritance resolution."""

import os
from pathlib import Path
from typing import Any, Dict, Set

import yaml

from dataloader.core.exceptions import RecipeError
from dataloader.models.merger import apply_delete_semantics, merge_recipes
from dataloader.models.recipe import Recipe
from dataloader.models.templates import render_templates
from dataloader.transforms.loader import load_custom_transforms


def load_recipe(path: str, cli_vars: Dict[str, str] | None = None) -> Recipe:
    """
    Load recipe from YAML file with inheritance resolution.

    Args:
        path: Path to recipe YAML file
        cli_vars: Variables passed via CLI (e.g., --vars key=value)

    Returns:
        Fully resolved Recipe instance

    Raises:
        RecipeError: If file not found, invalid YAML, validation fails, or cycle detected
    """
    recipe_path = Path(path)
    if not recipe_path.exists():
        raise RecipeError(f"Recipe file not found: {path}")

    visited: Set[str] = set()
    recipe_dict = _load_recipe_recursive(recipe_path, visited, recipe_path.parent)

    recipe_dict = render_templates(recipe_dict, cli_vars)

    # Load custom transforms specified under runtime.custom_transforms
    runtime_cfg = recipe_dict.get("runtime", {}) or {}
    custom_modules = runtime_cfg.get("custom_transforms", [])
    if custom_modules:
        base_dir = recipe_path.parent
        resolved = [
            str((base_dir / p).resolve()) if not os.path.isabs(p) else p
            for p in custom_modules
        ]
        load_custom_transforms(resolved)

    try:
        return Recipe.from_dict(recipe_dict)
    except Exception as e:
        raise RecipeError(
            f"Recipe validation failed: {e}", context={"path": str(path)}
        ) from e


def _load_recipe_recursive(
    recipe_path: Path, visited: Set[str], base_dir: Path
) -> Dict[str, Any]:
    """
    Recursively load recipe and resolve inheritance chain.

    Args:
        recipe_path: Path to current recipe file
        visited: Set of already visited recipe paths (for cycle detection)
        base_dir: Base directory for resolving relative paths

    Returns:
        Fully resolved recipe dictionary

    Raises:
        RecipeError: If cycle detected, file not found, or invalid YAML
    """
    abs_path = recipe_path.resolve()
    abs_path_str = str(abs_path)

    if abs_path_str in visited:
        cycle = " -> ".join(sorted(visited)) + f" -> {abs_path_str}"
        raise RecipeError(
            f"Cycle detected in recipe inheritance: {cycle}",
            context={"path": str(recipe_path)},
        )

    visited.add(abs_path_str)

    try:
        with open(abs_path, "r", encoding="utf-8") as f:
            recipe_dict = yaml.safe_load(f)
    except FileNotFoundError:
        raise RecipeError(
            f"Recipe file not found: {abs_path}", context={"path": str(recipe_path)}
        )
    except yaml.YAMLError as e:
        raise RecipeError(
            f"Invalid YAML in recipe file: {e}", context={"path": str(recipe_path)}
        ) from e

    if not isinstance(recipe_dict, dict):
        raise RecipeError(
            f"Recipe file must contain a YAML dictionary",
            context={"path": str(recipe_path)},
        )

    extends = recipe_dict.get("extends")
    if extends:
        parent_path = _resolve_extends_path(extends, abs_path.parent)
        parent_dict = _load_recipe_recursive(parent_path, visited.copy(), base_dir)
        recipe_dict = merge_recipes(parent_dict, recipe_dict)

    delete_paths = recipe_dict.pop("delete", [])
    if delete_paths:
        recipe_dict = apply_delete_semantics(recipe_dict, delete_paths)

    visited.remove(abs_path_str)

    return recipe_dict


def _resolve_extends_path(extends: str, current_dir: Path) -> Path:
    """
    Resolve extends path relative to current recipe directory.

    Args:
        extends: Path from extends field (may be relative or absolute)
        current_dir: Directory of current recipe file

    Returns:
        Resolved Path object

    Raises:
        RecipeError: If path cannot be resolved
    """
    if os.path.isabs(extends):
        path = Path(extends)
    else:
        path = (current_dir / extends).resolve()

    if not path.exists():
        raise RecipeError(
            f"Parent recipe not found: {extends}",
            context={"current_dir": str(current_dir), "extends": extends},
        )

    return path


def from_yaml(path: str, cli_vars: Dict[str, str] | None = None) -> Recipe:
    """
    Alias for load_recipe for backward compatibility.

    Args:
        path: Path to recipe YAML file
        cli_vars: Variables passed via CLI (e.g., --vars key=value)

    Returns:
        Fully resolved Recipe instance
    """
    return load_recipe(path, cli_vars)
