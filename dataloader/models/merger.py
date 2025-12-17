"""Deep merge algorithm for recipe inheritance."""

from typing import Any, Dict, List


def merge_recipes(parent: Dict[str, Any], child: Dict[str, Any]) -> Dict[str, Any]:
    """
    Deep merge parent recipe into child recipe.

    Rules:
    - Scalars: child overrides parent
    - Dicts: deep merge (recursive)
    - Lists: special handling
      - transform.steps: concatenate (parent steps + child steps)
      - Other lists: child overrides (for v0.1)
    - Missing keys: inherit from parent

    Args:
        parent: Parent recipe dictionary
        child: Child recipe dictionary

    Returns:
        Merged recipe dictionary
    """
    result = parent.copy()
    
    child_delete = child.get("delete")
    if child_delete is not None:
        result["delete"] = child_delete

    for key, child_value in child.items():
        if key == "delete":
            continue

        if key not in result:
            result[key] = child_value
        elif isinstance(child_value, dict) and isinstance(result[key], dict):
            if key == "transform":
                result[key] = _merge_transform_config(result[key], child_value)
            else:
                result[key] = merge_recipes(result[key], child_value)
        elif isinstance(child_value, list) and isinstance(result[key], list):
            result[key] = child_value
        else:
            result[key] = child_value

    return result


def _merge_transform_config(
    parent_transform: Dict[str, Any], child_transform: Dict[str, Any]
) -> Dict[str, Any]:
    """Merge transform configs, concatenating steps lists."""
    merged = parent_transform.copy()

    for key, child_value in child_transform.items():
        if key == "steps" and isinstance(child_value, list):
            parent_steps = merged.get("steps", [])
            merged["steps"] = parent_steps + child_value
        elif isinstance(child_value, dict) and isinstance(merged.get(key), dict):
            merged[key] = merge_recipes(merged[key], child_value)
        else:
            merged[key] = child_value

    return merged


def apply_delete_semantics(recipe: Dict[str, Any], delete_paths: List[str]) -> Dict[str, Any]:
    """
    Apply delete semantics to remove specified keys from recipe.

    Args:
        recipe: Recipe dictionary
        delete_paths: List of dot-separated paths to delete (e.g., ['transform.steps'])

    Returns:
        Recipe dictionary with specified paths removed
    """
    import copy

    result = copy.deepcopy(recipe)

    for path in delete_paths:
        _delete_path(result, path)

    return result


def _delete_path(data: Dict[str, Any], path: str) -> None:
    """Delete a nested path from dictionary using dot notation."""
    parts = path.split(".")
    current = data

    for i, part in enumerate(parts[:-1]):
        if part not in current or not isinstance(current[part], dict):
            return
        current = current[part]

    if parts[-1] in current:
        del current[parts[-1]]

