"""Core execution engine for recipe-based data loading."""

import logging
from typing import Any

from dataloader.core.batch import Batch
from dataloader.core.exceptions import EngineError
from dataloader.core.state import State
from dataloader.core.state_backend import StateBackend

logger = logging.getLogger(__name__)


def execute(
    recipe: Any,  # Recipe type will be defined in models phase
    state_backend: StateBackend,
    connections: dict[str, Any],
) -> None:
    """Execute a recipe to load data from source to destination.

    Core execution loop:
    1. Load state for the recipe
    2. Get source, transformer, and destination from recipe
    3. Iterate over batches from source
    4. Apply transforms to each batch
    5. Write batch to destination
    6. Save state after each batch

    Args:
        recipe: Recipe object containing source, transform, and destination configs
        state_backend: Backend for loading and saving state
        connections: Dictionary of connection configurations keyed by conn_id

    Raises:
        EngineError: If execution fails at any step
    """
    logger.info(f"Starting execution for recipe: {recipe.name}")

    try:
        state_dict = state_backend.load(recipe.name)
        state = State.from_dict(state_dict)

        logger.debug(f"Loaded state for recipe {recipe.name}: {state.to_dict()}")

        source = _get_source(recipe.source, connections)
        transformer = _get_transformer(recipe.transform)
        destination = _get_destination(recipe.destination, connections)

        batch_count = 0
        total_rows = 0

        for batch in source.read_batches(state):
            batch_count += 1
            total_rows += batch.row_count

            logger.info(
                f"Processing batch {batch_count} with {batch.row_count} rows "
                f"for recipe {recipe.name}"
            )

            batch = transformer.apply(batch)
            destination.write_batch(batch, state)

            state_backend.save(recipe.name, state.to_dict())

            logger.debug(f"Saved state after batch {batch_count}")

        logger.info(
            f"Completed execution for recipe {recipe.name}: "
            f"{batch_count} batches, {total_rows} total rows"
        )

    except Exception as e:
        error_msg = f"Execution failed for recipe {recipe.name}: {e}"
        logger.error(error_msg, exc_info=True)
        raise EngineError(
            error_msg,
            context={"recipe_name": recipe.name},
        ) from e


def _get_source(source_config: Any, connections: dict[str, Any]) -> Any:
    """Get source connector instance from config.

    Args:
        source_config: Source configuration from recipe
        connections: Dictionary of connection configurations

    Returns:
        Source connector instance

    Raises:
        EngineError: If source cannot be created
    """
    # Placeholder implementation - will be implemented in connectors phase
    raise NotImplementedError("Source connector resolution not yet implemented")


def _get_transformer(transform_config: Any) -> Any:
    """Get transformer instance from config.

    Args:
        transform_config: Transform configuration from recipe

    Returns:
        Transformer instance

    Raises:
        EngineError: If transformer cannot be created
    """
    # Placeholder implementation - will be implemented in transforms phase
    raise NotImplementedError("Transformer resolution not yet implemented")


def _get_destination(destination_config: Any, connections: dict[str, Any]) -> Any:
    """Get destination connector instance from config.

    Args:
        destination_config: Destination configuration from recipe
        connections: Dictionary of connection configurations

    Returns:
        Destination connector instance

    Raises:
        EngineError: If destination cannot be created
    """
    # Placeholder implementation - will be implemented in connectors phase
    raise NotImplementedError("Destination connector resolution not yet implemented")

