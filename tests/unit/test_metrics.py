"""Unit tests for metrics collection."""

import time

from dataloader.core.metrics import MetricsCollector


def test_metrics_collector_basic():
    """Test basic metrics collection."""
    metrics = MetricsCollector("test_recipe")

    # Record some batches
    metrics.record_batch(100, 0.5)
    metrics.record_batch(200, 0.8)
    metrics.record_batch(150, 0.6)

    # Add a small sleep to ensure measurable execution time
    time.sleep(0.01)

    metrics.finish()

    assert metrics.batches_processed == 3
    assert metrics.rows_processed == 450
    assert metrics.errors == 0
    assert metrics.execution_time > 0


def test_metrics_collector_errors():
    """Test error recording."""
    metrics = MetricsCollector("test_recipe")

    metrics.record_batch(100, 0.5)
    metrics.record_error(ValueError("Test error"), {"batch_id": 1})
    metrics.record_batch(200, 0.8)

    metrics.finish()

    assert metrics.batches_processed == 2
    assert metrics.rows_processed == 300
    assert metrics.errors == 1
    assert len(metrics.error_details) == 1
    assert metrics.error_details[0]["error_type"] == "ValueError"


def test_metrics_collector_to_dict():
    """Test metrics export to dictionary."""
    metrics = MetricsCollector("test_recipe")

    metrics.record_batch(100, 0.5)
    metrics.record_batch(200, 0.8)
    metrics.finish()

    metrics_dict = metrics.to_dict()

    assert metrics_dict["recipe_name"] == "test_recipe"
    assert metrics_dict["batches_processed"] == 2
    assert metrics_dict["rows_processed"] == 300
    assert metrics_dict["errors"] == 0
    assert "execution_time" in metrics_dict
    assert "rows_per_second" in metrics_dict
    assert "avg_batch_time" in metrics_dict


def test_metrics_collector_summary():
    """Test metrics summary string."""
    metrics = MetricsCollector("test_recipe")

    metrics.record_batch(100, 0.5)
    metrics.finish()

    summary = metrics.get_summary()

    assert "test_recipe" in summary
    assert "Batches: 1" in summary
    assert "Rows: 100" in summary
