"""Core execution engine for recipe-based data loading."""

import logging
from typing import Any

from dataloader.connectors import get_destination, get_source
from dataloader.core.batch import Batch
from dataloader.core.exceptions import EngineError
from dataloader.core.state import State
from dataloader.core.state_backend import StateBackend
from dataloader.models.destination_config import DestinationConfig
from dataloader.models.source_config import SourceConfig
from dataloader.models.transform_config import TransformConfig
from dataloader.transforms import TransformPipeline

logger = logging.getLogger(__name__)


def execute(
    recipe: Any,  # Recipe type will be defined in models phase
    state_backend: StateBackend,
) -> None:
    """Execute a recipe to load data from source to destination.

    Core execution loop:
    1. Load state for the recipe
    2. Get source, transformer, and destination from recipe
    3. Iterate over batches from source
    4. Apply transforms to each batch
    5. Write batch to destination
    6. Save state after each batch

    All connection parameters come from the recipe configuration, which supports
    Jinja2-style templates (e.g., {{ env_var('DB_HOST') }}) that are rendered
    during recipe loading.

    Args:
        recipe: Recipe object containing source, transform, and destination configs.
                Source and destination configuration come from recipe.source and
                recipe.destination respectively. Connection parameters (host, user,
                password, etc.) are specified in the recipe and can use templates.
        state_backend: Backend for loading and saving state

    Raises:
        EngineError: If execution fails at any step
    """
    logger.info(f"Starting execution for recipe: {recipe.name}")

    try:
        state_dict = state_backend.load(recipe.name)
        state = State.from_dict(state_dict)

        logger.debug(f"Loaded state for recipe {recipe.name}: {state.to_dict()}")

        source = _get_source(recipe.source)
        transformer = _get_transformer(recipe.transform)
        destination = _get_destination(recipe.destination)

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


def _get_source(source_config: SourceConfig) -> Any:
    """Get source connector instance from config.

    All connection parameters come from the source_config, which has already
    had templates rendered during recipe loading.

    Args:
        source_config: Source configuration from recipe (templates already rendered)

    Returns:
        Source connector instance

    Raises:
        EngineError: If source cannot be created
    """
    try:
        # All connection parameters come from the config
        # Connectors expect an empty connection dict since config has all values
        return get_source(source_config.type, source_config, {})
    except Exception as e:
        raise EngineError(
            f"Failed to create source connector: {e}",
            context={"source_type": source_config.type},
        ) from e


def _get_transformer(transform_config: TransformConfig) -> TransformPipeline:
    """Get transformer instance from config.

    Args:
        transform_config: Transform configuration from recipe

    Returns:
        Transformer instance

    Raises:
        EngineError: If transformer cannot be created
    """
    try:
        return TransformPipeline(transform_config)
    except Exception as e:
        raise EngineError(
            f"Failed to create transform pipeline: {e}",
            context={"transform_steps": len(transform_config.steps)},
        ) from e


def _get_destination(destination_config: DestinationConfig) -> Any:
    """Get destination connector instance from config.

    All connection parameters come from the destination_config, which has already
    had templates rendered during recipe loading.

    Args:
        destination_config: Destination configuration from recipe (templates already rendered)

    Returns:
        Destination connector instance

    Raises:
        EngineError: If destination cannot be created
    """
    try:
        # All connection parameters come from the config
        # Connectors expect an empty connection dict since config has all values
        return get_destination(destination_config.type, destination_config, {})
    except Exception as e:
        raise EngineError(
            f"Failed to create destination connector: {e}",
            context={"destination_type": destination_config.type},
        ) from e

