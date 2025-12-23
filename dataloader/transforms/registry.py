"""Transform registry for managing transform factories."""

from typing import Any, Callable, Protocol, overload, runtime_checkable

from dataloader.core.batch import Batch
from dataloader.core.exceptions import TransformError


@runtime_checkable
class BaseTransform(Protocol):
    """Protocol for class-based transforms."""

    def apply(self, batch: Batch) -> Batch:
        """Apply transform to batch and return modified batch."""
        ...


Transform = BaseTransform
TransformFunc = Callable[[Batch, dict[str, Any]], Batch]
TransformFactory = Callable[[dict[str, Any]], Transform | TransformFunc]

_transform_registry: dict[str, TransformFactory] = {}


@overload
def register_transform(
    transform_type: str,
) -> Callable[[TransformFactory], TransformFactory]: ...


@overload
def register_transform(transform_type: str, factory: TransformFactory) -> None: ...


def register_transform(
    transform_type: str,
    factory: TransformFactory | None = None,
) -> Callable[[TransformFactory], TransformFactory] | None:
    """Register a transform factory.

    Can be used as a decorator or called directly:

        # As decorator
        @register_transform("rename_columns")
        def create_rename_transform(config):
            return RenameColumnsTransform(config)

        # Direct call
        register_transform("rename_columns", create_rename_transform)

    Args:
        transform_type: Unique identifier for the transform (e.g., 'rename_columns').
        factory: Factory function (optional if used as decorator).

    Raises:
        TransformError: If a transform with the same type is already registered.
    """

    def _register(f: TransformFactory) -> TransformFactory:
        if transform_type in _transform_registry:
            raise TransformError(
                f"Transform '{transform_type}' is already registered",
                context={"transform_type": transform_type},
            )
        _transform_registry[transform_type] = f
        return f

    if factory is not None:
        _register(factory)
        return None

    return _register


def get_transform(
    transform_type: str,
    config: dict[str, Any],
) -> Transform | TransformFunc:
    """Create a transform instance using the registered factory.

    Args:
        transform_type: The transform type to instantiate.
        config: Transform configuration from the recipe step.

    Returns:
        A Transform instance or callable.

    Raises:
        TransformError: If the transform type is not registered.
    """
    factory = _transform_registry.get(transform_type)
    if factory is None:
        available = ", ".join(sorted(_transform_registry.keys())) or "(none)"
        raise TransformError(
            f"Unknown transform type: '{transform_type}'",
            context={"transform_type": transform_type, "available_types": available},
        )
    return factory(config)


def list_transform_types() -> list[str]:
    """Return a list of all registered transform types.

    Returns:
        Sorted list of transform type strings.
    """
    return sorted(_transform_registry.keys())


def clear_registry() -> None:
    """Clear all registered transforms.

    Intended for testing only. Removes all transform registrations.
    """
    _transform_registry.clear()
