"""Transform pipeline executor for applying transform steps to batches."""

from typing import Any

from dataloader.core.batch import ArrowBatch, Batch
from dataloader.core.exceptions import TransformError
from dataloader.models.transform_config import TransformConfig, TransformStep
from dataloader.transforms.registry import Transform, get_transform


class TransformPipeline:
    """Executes transform steps sequentially on batches.

    Each step receives a batch and returns a modified batch.
    Pipeline preserves batch metadata through transformations.
    """

    def __init__(self, config: TransformConfig):
        """Initialize pipeline with transform configuration.

        Args:
            config: TransformConfig containing list of steps to apply.
        """
        self._steps = config.steps

    def apply(self, batch: Batch) -> Batch:
        """Apply all transform steps sequentially to batch.

        Args:
            batch: Input batch to transform.

        Returns:
            Transformed batch with all steps applied.

        Raises:
            TransformError: If any step fails, with step context included.
        """
        current_batch = batch

        for step_index, step in enumerate(self._steps):
            current_batch = self._apply_step(current_batch, step, step_index)
            self._validate_batch_structure(current_batch, step_index)

        return current_batch

    def _apply_step(
        self,
        batch: Batch,
        step: TransformStep,
        step_index: int,
    ) -> Batch:
        """Apply a single transform step to batch.

        Args:
            batch: Current batch state.
            step: Transform step configuration.
            step_index: Index of step for error context.

        Returns:
            Transformed batch.

        Raises:
            TransformError: If transform execution fails.
        """
        transform_type = step.type
        step_config = self._extract_step_config(step)

        try:
            transform = get_transform(transform_type, step_config)
            return self._execute_transform(batch, transform, step_config)
        except TransformError:
            raise
        except Exception as e:
            raise TransformError(
                f"Transform failed at step {step_index}",
                context={
                    "step_index": step_index,
                    "transform_type": transform_type,
                    "error": str(e),
                },
            ) from e

    def _execute_transform(
        self,
        batch: Batch,
        transform: Transform | Any,
        config: dict[str, Any],
    ) -> Batch:
        """Execute transform on batch.

        Handles both class-based (with apply method) and function-based transforms.

        Args:
            batch: Input batch.
            transform: Transform instance or callable.
            config: Step configuration for function-based transforms.

        Returns:
            Transformed batch.
        """
        if hasattr(transform, "apply") and callable(transform.apply):
            return transform.apply(batch)
        elif callable(transform):
            return transform(batch, config)
        else:
            raise TransformError(
                "Invalid transform: must be callable or have apply() method",
                context={"transform_type": type(transform).__name__},
            )

    def _extract_step_config(self, step: TransformStep) -> dict[str, Any]:
        """Extract configuration dict from step, excluding 'type' field."""
        step_dict = step.model_dump()
        step_dict.pop("type", None)
        return step_dict

    def _validate_batch_structure(self, batch: Batch, step_index: int) -> None:
        """Validate batch structure after transform step.

        Args:
            batch: Batch to validate.
            step_index: Index of step that produced this batch.

        Raises:
            TransformError: If batch structure is invalid.
        """
        if not batch.columns:
            raise TransformError(
                "Batch has no columns after transform",
                context={"step_index": step_index},
            )

        expected_column_count = len(batch.columns)
        for row_index, row in enumerate(batch.rows):
            if len(row) != expected_column_count:
                raise TransformError(
                    f"Row {row_index} has {len(row)} values but expected {expected_column_count}",
                    context={
                        "step_index": step_index,
                        "row_index": row_index,
                        "expected": expected_column_count,
                        "actual": len(row),
                    },
                )

