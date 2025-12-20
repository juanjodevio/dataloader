"""Structured logging configuration for dataloader."""

import logging
import sys
from typing import Optional

try:
    from json_log_formatter import JSONFormatter
except ImportError:
    JSONFormatter = None  # type: ignore


def configure_logging(
    level: str = "INFO",
    json_format: bool = False,
    recipe_name: Optional[str] = None,
) -> None:
    """Configure logging for dataloader.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        json_format: If True, use JSON format; otherwise use normal format
        recipe_name: Optional recipe name to include in log context
    """
    log_level = getattr(logging, level.upper(), logging.INFO)

    # Get root logger
    logger = logging.getLogger("dataloader")
    logger.setLevel(log_level)

    # Remove existing handlers
    logger.handlers.clear()

    # Create handler
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(log_level)

    # Set formatter
    if json_format:
        if JSONFormatter is None:
            raise ImportError(
                "json-log-formatter is required for JSON logging. "
                "Install it with: pip install json-log-formatter"
            )
        formatter = JSONFormatter()
    else:
        formatter = StructuredFormatter()

    handler.setFormatter(formatter)
    logger.addHandler(handler)


class StructuredFormatter(logging.Formatter):
    """Structured formatter that adds context to log messages."""

    def format(self, record: logging.LogRecord) -> str:
        """Format log record with context."""
        # Extract context from extra dict
        context = getattr(record, "context", {})
        
        # Build message parts
        parts = [f"[{record.levelname}]"]
        
        if hasattr(record, "recipe_name"):
            parts.append(f"recipe={record.recipe_name}")
        
        if hasattr(record, "batch_id"):
            parts.append(f"batch={record.batch_id}")
        
        # Add any additional context
        for key, value in context.items():
            parts.append(f"{key}={value}")
        
        parts.append(record.getMessage())
        
        return " ".join(parts)



