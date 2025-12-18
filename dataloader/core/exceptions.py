"""Exception hierarchy for the dataloader package."""


class DataLoaderError(Exception):
    """Base exception for all dataloader errors."""

    def __init__(self, message: str, context: dict | None = None):
        super().__init__(message)
        self.message = message
        self.context = context or {}

    def __str__(self) -> str:
        if self.context:
            context_str = ", ".join(f"{k}={v}" for k, v in self.context.items())
            return f"{self.message} ({context_str})"
        return self.message


class RecipeError(DataLoaderError):
    """Raised when recipe parsing or validation fails."""

    pass


class ConnectorError(DataLoaderError):
    """Raised when connector operations fail."""

    pass


class TransformError(DataLoaderError):
    """Raised when transform execution fails."""

    pass


class StateError(DataLoaderError):
    """Raised when state backend operations fail."""

    pass


class EngineError(DataLoaderError):
    """Raised when execution engine operations fail."""

    pass

