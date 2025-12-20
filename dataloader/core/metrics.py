"""Metrics collection for dataloader execution."""

import time
from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class MetricsCollector:
    """Collects metrics during recipe execution."""

    recipe_name: str
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    
    batches_processed: int = 0
    rows_processed: int = 0
    errors: int = 0
    execution_time: float = 0.0
    
    batch_times: list[float] = field(default_factory=list)
    error_details: list[dict[str, Any]] = field(default_factory=list)
    
    def record_batch(self, row_count: int, batch_time: float) -> None:
        """Record a processed batch.
        
        Args:
            row_count: Number of rows in the batch
            batch_time: Time taken to process the batch in seconds
        """
        self.batches_processed += 1
        self.rows_processed += row_count
        self.batch_times.append(batch_time)
    
    def record_error(self, error: Exception, context: dict[str, Any] | None = None) -> None:
        """Record an error.
        
        Args:
            error: The exception that occurred
            context: Additional context about the error
        """
        self.errors += 1
        error_detail = {
            "error_type": type(error).__name__,
            "error_message": str(error),
        }
        if context:
            error_detail["context"] = context
        self.error_details.append(error_detail)
    
    def finish(self) -> None:
        """Mark execution as finished and calculate final metrics."""
        self.end_time = time.time()
        self.execution_time = self.end_time - self.start_time
    
    def to_dict(self) -> dict[str, Any]:
        """Export metrics as dictionary.
        
        Returns:
            Dictionary containing all metrics
        """
        avg_batch_time = (
            sum(self.batch_times) / len(self.batch_times)
            if self.batch_times
            else 0.0
        )
        rows_per_second = (
            self.rows_processed / self.execution_time
            if self.execution_time > 0
            else 0.0
        )
        
        return {
            "recipe_name": self.recipe_name,
            "execution_time": self.execution_time,
            "batches_processed": self.batches_processed,
            "rows_processed": self.rows_processed,
            "errors": self.errors,
            "avg_batch_time": avg_batch_time,
            "rows_per_second": rows_per_second,
            "error_details": self.error_details,
        }
    
    def get_summary(self) -> str:
        """Get human-readable summary of metrics.
        
        Returns:
            Summary string
        """
        if not self.end_time:
            self.finish()
        
        metrics = self.to_dict()
        summary_parts = [
            f"Recipe: {metrics['recipe_name']}",
            f"Batches: {metrics['batches_processed']}",
            f"Rows: {metrics['rows_processed']}",
            f"Time: {metrics['execution_time']:.2f}s",
            f"Rate: {metrics['rows_per_second']:.0f} rows/s",
        ]
        
        if metrics["errors"] > 0:
            summary_parts.append(f"Errors: {metrics['errors']}")
        
        return " | ".join(summary_parts)

