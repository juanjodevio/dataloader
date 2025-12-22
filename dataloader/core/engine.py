"""Core execution engine for recipe-based data loading."""

import asyncio
import logging
import time
from typing import Any, Optional

from dataloader.connectors import get_connector
from dataloader.core.batch import ArrowBatch, Batch
from dataloader.core.exceptions import EngineError
from dataloader.core.metrics import MetricsCollector
from dataloader.core.parallel import AsyncParallelExecutor, run_async
from dataloader.core.schema import (
    InMemorySchemaStorage,
    Schema,
    SchemaEvolution,
    SchemaRegistry,
    SchemaValidator,
)
from dataloader.core.state import State
from dataloader.core.state_backend import StateBackend
from dataloader.models.destination_config import DestinationConfig
from dataloader.models.recipe import Recipe
from dataloader.models.schema_config import SchemaConfig
from dataloader.models.source_config import SourceConfig
from dataloader.models.transform_config import TransformConfig
from dataloader.transforms import TransformPipeline

logger = logging.getLogger(__name__)


def execute(
    recipe: Recipe,
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
    # Check if parallelism is enabled
    parallelism = (
        recipe.runtime.parallelism if hasattr(recipe.runtime, "parallelism") else 1
    )

    if parallelism > 1:
        # Use async execution for parallelism
        run_async(_execute_async(recipe, state_backend))
    else:
        # Use sequential execution
        _execute_sequential(recipe, state_backend)


def _execute_sequential(
    recipe: Recipe,
    state_backend: StateBackend,
) -> None:
    """Execute recipe sequentially (one batch at a time)."""
    metrics = MetricsCollector(recipe.name)

    logger.info("Starting execution", extra={"recipe_name": recipe.name})

    try:
        state_dict = state_backend.load(recipe.name)
        state = State.from_dict(state_dict)

        logger.debug(
            f"Loaded state: {state.to_dict()}", extra={"recipe_name": recipe.name}
        )

        source = _get_connector(recipe.source)
        transformer = _get_transformer(recipe.transform)
        destination = _get_connector(recipe.destination)

        schema_validator, current_schema, evolution, registry = _build_schema_context(
            recipe.name, recipe.schema_config
        )

        for batch in source.read_batches(state):
            batch_start = time.time()

            logger.info(
                f"Processing batch with {batch.row_count} rows",
                extra={
                    "recipe_name": recipe.name,
                    "batch_id": metrics.batches_processed + 1,
                },
            )

            try:
                if schema_validator and current_schema:
                    batch, current_schema = _validate_batch(
                        recipe.name,
                        batch,
                        current_schema,
                        schema_validator,
                        evolution,
                        registry,
                    )
                    if batch.row_count == 0:
                        continue

                batch = transformer.apply(batch)
                destination.write_batch(batch, state)

                batch_time = time.time() - batch_start
                metrics.record_batch(batch.row_count, batch_time)

                state_backend.save(recipe.name, state.to_dict())

                logger.debug(
                    "Saved state after batch",
                    extra={
                        "recipe_name": recipe.name,
                        "batch_id": metrics.batches_processed,
                    },
                )
            except Exception as e:
                metrics.record_error(e, {"batch_id": metrics.batches_processed + 1})
                raise

        metrics.finish()
        logger.info(
            f"Completed execution: {metrics.get_summary()}",
            extra={"recipe_name": recipe.name},
        )

        # Save metrics to state metadata
        state_dict = state_backend.load(recipe.name)
        state = State.from_dict(state_dict)
        state = state.update(
            metadata={**state.metadata, "last_metrics": metrics.to_dict()}
        )
        state_backend.save(recipe.name, state.to_dict())

    except Exception as e:
        metrics.record_error(e)
        metrics.finish()
        error_msg = f"Execution failed: {e}"
        logger.error(error_msg, extra={"recipe_name": recipe.name}, exc_info=True)
        raise EngineError(
            error_msg,
            context={"recipe_name": recipe.name, "metrics": metrics.to_dict()},
        ) from e


async def _execute_async(
    recipe: Recipe,
    state_backend: StateBackend,
) -> None:
    """Execute recipe with async parallelism."""
    metrics = MetricsCollector(recipe.name)
    parallelism = recipe.runtime.parallelism

    logger.info(
        f"Starting execution with parallelism={parallelism}",
        extra={"recipe_name": recipe.name},
    )

    try:
        # Load state (async if available)
        if hasattr(state_backend, "load_async"):
            state_dict = await state_backend.load_async(recipe.name)
        else:
            # Run sync load in thread
            loop = asyncio.get_event_loop()
            state_dict = await loop.run_in_executor(
                None, state_backend.load, recipe.name
            )

        state = State.from_dict(state_dict)

        logger.debug(
            f"Loaded state: {state.to_dict()}",
            extra={"recipe_name": recipe.name},
        )

        source = _get_connector(recipe.source)
        transformer = _get_transformer(recipe.transform)
        destination = _get_connector(recipe.destination)

        schema_validator, current_schema, evolution, registry = _build_schema_context(
            recipe.name, recipe.schema_config
        )

        # Collect all batches first (needed for parallel processing)
        batches = list(source.read_batches(state))

        if not batches:
            logger.info("No batches to process", extra={"recipe_name": recipe.name})
            return

        # Process batches in parallel with semaphore control
        semaphore = asyncio.Semaphore(parallelism)

        async def process_batch_with_id(batch: Batch, batch_id: int) -> None:
            """Process a single batch (async wrapper)."""
            async with semaphore:
                batch_start = time.time()

                logger.info(
                    f"Processing batch with {batch.row_count} rows",
                    extra={
                        "recipe_name": recipe.name,
                        "batch_id": batch_id,
                    },
                )

                try:
                    if schema_validator and current_schema:
                        batch_, current_schema = _validate_batch(
                            recipe.name,
                            batch,
                            current_schema,
                            schema_validator,
                            evolution,
                            registry,
                        )
                        if batch_.row_count == 0:
                            return
                    else:
                        batch_ = batch

                    # Transform (sync, run in executor)
                    loop = asyncio.get_event_loop()
                    transformed_batch = await loop.run_in_executor(
                        None, transformer.apply, batch_
                    )

                    # Write (sync, run in executor)
                    await loop.run_in_executor(
                        None, destination.write_batch, transformed_batch, state
                    )

                    batch_time = time.time() - batch_start
                    metrics.record_batch(batch.row_count, batch_time)

                except Exception as e:
                    metrics.record_error(e, {"batch_id": batch_id})
                    raise

        # Create tasks for all batches
        tasks = [process_batch_with_id(batch, i + 1) for i, batch in enumerate(batches)]

        # Process all batches with concurrency control
        await asyncio.gather(*tasks)

        # Save state after all batches complete
        # Update state with metrics
        updated_state = state.update(
            metadata={**state.metadata, "last_metrics": metrics.to_dict()}
        )
        state_dict_to_save = updated_state.to_dict()

        if hasattr(state_backend, "save_async"):
            await state_backend.save_async(recipe.name, state_dict_to_save)
        else:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None, state_backend.save, recipe.name, state_dict_to_save
            )

        metrics.finish()
        logger.info(
            f"Completed execution: {metrics.get_summary()}",
            extra={"recipe_name": recipe.name},
        )

    except Exception as e:
        metrics.record_error(e)
        metrics.finish()
        error_msg = f"Execution failed: {e}"
        logger.error(error_msg, extra={"recipe_name": recipe.name}, exc_info=True)
        raise EngineError(
            error_msg,
            context={"recipe_name": recipe.name, "metrics": metrics.to_dict()},
        ) from e


def _get_connector(config: SourceConfig | DestinationConfig) -> Any:
    """Get connector instance from config (unified for source and destination).

    All connection parameters come from the config, which has already
    had templates rendered during recipe loading.

    Args:
        config: Source or destination configuration from recipe (templates already rendered)

    Returns:
        Connector instance (supports both read and write operations)

    Raises:
        EngineError: If connector cannot be created
    """
    try:
        # All connection parameters come from the config
        # The unified registry handles both source and destination configs
        return get_connector(config.type, config)
    except Exception as e:
        config_type = "source" if isinstance(config, SourceConfig) else "destination"
        raise EngineError(
            f"Failed to create {config_type} connector: {e}",
            context={f"{config_type}_type": config.type},
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


def _build_schema_context(
    recipe_name: str, schema_config: Optional[SchemaConfig]
) -> tuple[Optional[SchemaValidator], Optional[Schema], Optional[SchemaEvolution], Optional[SchemaRegistry]]:
    if not schema_config:
        return None, None, None, None
    validator = SchemaValidator(
        mode=schema_config.mode,
        contracts=schema_config.contracts,
    )
    base_schema = schema_config.to_schema()
    registry = SchemaRegistry(
        storage=InMemorySchemaStorage(),
        evolution=SchemaEvolution(),
    )
    registry.register(recipe_name, base_schema, version=schema_config.version or "initial")
    return validator, base_schema, registry.evolution, registry


def _validate_batch(
    recipe_name: str,
    batch: Batch,
    schema: Schema,
    validator: SchemaValidator,
    evolution: Optional[SchemaEvolution],
    registry: Optional[SchemaRegistry],
) -> tuple[Batch, Schema]:
    if not hasattr(batch, "to_arrow"):
        raise EngineError(
            "Schema validation requires Arrow-compatible batch",
            context={"recipe_name": recipe_name},
        )

    table = batch.to_arrow()
    result = validator.validate(table, schema)

    if not result.ok:
        messages = "; ".join(issue.message for issue in result.errors)
        raise EngineError(
            f"Schema validation failed: {messages}",
            context={"recipe_name": recipe_name},
        )

    new_table = table
    if result.dropped_rows:
        new_table = new_table.slice(0, 0)
    elif result.dropped_columns:
        new_table = new_table.drop(result.dropped_columns)

    new_schema = result.validated_schema

    if evolution and registry:
        new_schema, _ = evolution.apply(schema, new_schema)
        registry.register(recipe_name, new_schema)

    new_batch = ArrowBatch(new_table, metadata=getattr(batch, "metadata", {}))
    return new_batch, new_schema
