"""Template rendering for recipe values with Jinja2-style syntax."""

import os
import re
from typing import Any, Dict

from dataloader.core.exceptions import RecipeError


def render_templates(
    recipe_dict: Dict[str, Any], cli_vars: Dict[str, str] | None = None
) -> Dict[str, Any]:
    """
    Render Jinja2-style templates in recipe dictionary.

    Supports:
    - {{ env_var('VAR_NAME') }} - environment variable lookup
    - {{ var('VAR_NAME') }} - CLI variable lookup
    - {{ recipe.name }} - recipe metadata

    Args:
        recipe_dict: Recipe dictionary (may contain template expressions)
        cli_vars: Variables passed via CLI (e.g., --vars key=value)

    Returns:
        Recipe dictionary with templates rendered
    """
    recipe_name = recipe_dict.get("name", "")
    context = {
        "recipe": {"name": recipe_name},
        "env_var": lambda key: _get_env_var(key),
        "var": lambda key: _get_cli_var(key, cli_vars or {}),
    }

    return _render_dict(recipe_dict, context)


def _get_env_var(key: str) -> str:
    """Get environment variable or raise error if not found."""
    value = os.environ.get(key)
    if value is None:
        raise RecipeError(
            f"Environment variable '{key}' not found",
            context={"key": key},
        )
    return value


def _get_cli_var(key: str, cli_vars: Dict[str, str]) -> str:
    """Get CLI variable or raise error if not found."""
    if key not in cli_vars:
        raise RecipeError(
            f"CLI variable '{key}' not provided",
            context={"key": key, "available": list(cli_vars.keys())},
        )
    return cli_vars[key]


def _render_dict(data: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively render templates in dictionary."""
    result = {}
    for key, value in data.items():
        result[key] = _render_value(value, context)
    return result


def _render_value(value: Any, context: Dict[str, Any]) -> Any:
    """Render template in value based on type."""
    if isinstance(value, dict):
        return _render_dict(value, context)
    elif isinstance(value, list):
        return [_render_value(item, context) for item in value]
    elif isinstance(value, str):
        return _render_string(value, context)
    else:
        return value


def _render_string(text: str, context: Dict[str, Any]) -> str:
    """
    Render Jinja2-style templates in string.

    Supports:
    - {{ recipe.name }}
    - {{ env_var('KEY') }}
    - {{ var('KEY') }}
    """
    pattern = r"\{\{\s*([^}]+)\s*\}\}"

    def replace(match):
        expr = match.group(1).strip()
        try:
            # Handle function calls: func('arg') or func("arg")
            func_pattern = r"(\w+)\(['\"]([^'\"]+)['\"]\)"
            func_match = re.match(func_pattern, expr)
            if func_match:
                func_name = func_match.group(1)
                arg = func_match.group(2)
                if func_name in context and callable(context[func_name]):
                    return str(context[func_name](arg))
                else:
                    raise RecipeError(
                        f"Unknown function: {func_name}",
                        context={"expression": expr, "available": list(context.keys())},
                    )

            # Handle dot-notation: recipe.name
            parts = expr.split(".")
            result = context
            for part in parts:
                result = result[part]
            return str(result)
        except (KeyError, TypeError) as e:
            raise RecipeError(
                f"Template rendering failed: {expr}",
                context={"expression": expr, "error": str(e)},
            ) from e

    return re.sub(pattern, replace, text)
