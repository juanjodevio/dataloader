"""Transform pipeline module for data transformations.

Provides:
- TransformPipeline: Sequential executor for transform steps
- Transform registry: Registration and retrieval of transform factories
- Built-in transforms: rename_columns, cast, add_column
"""

# Registry must be imported first (other modules use register_transform decorator)
from dataloader.transforms.registry import (
    Transform,
    TransformFactory,
    TransformFunc,
    clear_registry,
    get_transform,
    list_transform_types,
    register_transform,
)

# Transform modules register themselves via @register_transform decorator
from dataloader.transforms.add_column import (
    AddColumnTransform,
    create_add_column_transform,
)
from dataloader.transforms.cast import CastColumnsTransform, create_cast_transform
from dataloader.transforms.pipeline import TransformPipeline
from dataloader.transforms.rename import (
    RenameColumnsTransform,
    create_rename_transform,
)

__all__ = [
    # Pipeline
    "TransformPipeline",
    # Registry
    "register_transform",
    "get_transform",
    "list_transform_types",
    "clear_registry",
    "Transform",
    "TransformFunc",
    "TransformFactory",
    # Transform classes
    "RenameColumnsTransform",
    "CastColumnsTransform",
    "AddColumnTransform",
    # Factory functions
    "create_rename_transform",
    "create_cast_transform",
    "create_add_column_transform",
]

