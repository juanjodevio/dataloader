"""Utilities to load custom transform modules."""

from __future__ import annotations

import importlib
import importlib.util
import inspect
from pathlib import Path
from typing import Any

from dataloader.core.exceptions import TransformError
from dataloader.transforms.registry import BaseTransform, register_transform


def load_custom_transforms_from_module(module_path: str) -> None:
    """Import a module so its @register_transform declarations run.

    Additionally, if the module defines BaseTransform subclasses that are
    not explicitly registered, register them using snake_case class names.
    """
    module = _import_module(module_path)

    for name, obj in inspect.getmembers(module):
        if (
            inspect.isclass(obj)
            and issubclass(obj, BaseTransform)
            and obj is not BaseTransform
        ):
            transform_type = _camel_to_snake(name)
            if transform_type:
                # register only if not already registered
                try:
                    register_transform(transform_type, lambda cfg, cls=obj: cls(cfg))
                except TransformError:
                    pass


def load_custom_transforms(paths: list[str]) -> None:
    """Load all custom transform modules from the provided paths."""
    for path in paths:
        load_custom_transforms_from_module(path)


def _camel_to_snake(name: str) -> str:
    out = ""
    for i, ch in enumerate(name):
        if ch.isupper() and i > 0:
            out += "_"
        out += ch.lower()
    return out


def _import_module(module_path: str):
    """Import by module path or file path."""
    path_obj = Path(module_path)
    if path_obj.suffix == ".py" or path_obj.exists():
        spec = importlib.util.spec_from_file_location(path_obj.stem, path_obj)
        if spec is None or spec.loader is None:
            raise TransformError(f"Cannot load module from path: {module_path}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)  # type: ignore[arg-type]
        return module

    try:
        return importlib.import_module(module_path)
    except ImportError as exc:
        raise TransformError(
            f"Failed to import custom transform module '{module_path}': {exc}"
        ) from exc
