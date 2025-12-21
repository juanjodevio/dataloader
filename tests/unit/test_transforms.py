"""Unit tests for transform pipeline and transforms."""

from datetime import datetime

import pytest

from dataloader.core.batch import ArrowBatch
from dataloader.core.exceptions import TransformError
from dataloader.models.transform_config import TransformConfig, TransformStep
from dataloader.transforms import (
    AddColumnTransform,
    CastColumnsTransform,
    RenameColumnsTransform,
    TransformPipeline,
    clear_registry,
    create_add_column_transform,
    create_cast_transform,
    create_rename_transform,
    get_transform,
    list_transform_types,
    register_transform,
)


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def sample_batch():
    """Create a sample batch for testing."""
    return ArrowBatch.from_rows(
        columns=["id", "name", "age", "created_at"],
        rows=[
            [1, "Alice", "25", "2024-01-01T10:00:00"],
            [2, "Bob", "30", "2024-01-02T11:00:00"],
            [3, "Charlie", "35", "2024-01-03T12:00:00"],
        ],
        metadata={"source": "test"},
    )


@pytest.fixture
def empty_batch():
    """Create an empty batch for testing."""
    return ArrowBatch.from_rows(
        columns=["id", "name"],
        rows=[],
        metadata={},
    )


@pytest.fixture(autouse=True)
def reset_registry():
    """Reset registry before each test and restore after."""
    # Store original state - imports register transforms, so we need to preserve
    original_types = list_transform_types()

    yield

    # Clear and re-register built-ins after test
    clear_registry()
    register_transform("rename_columns", create_rename_transform)
    register_transform("cast", create_cast_transform)
    register_transform("add_column", create_add_column_transform)


# ============================================================================
# Registry Tests
# ============================================================================


class TestTransformRegistry:
    """Tests for transform registry."""

    def test_list_transform_types_includes_builtins(self):
        """Built-in transforms should be registered."""
        types = list_transform_types()
        assert "rename_columns" in types
        assert "cast" in types
        assert "add_column" in types

    def test_get_transform_returns_instance(self):
        """get_transform should return a usable transform."""
        transform = get_transform("rename_columns", {"mapping": {"a": "b"}})
        assert hasattr(transform, "apply")

    def test_get_transform_unknown_type_raises(self):
        """Unknown transform type should raise TransformError."""
        with pytest.raises(TransformError) as exc_info:
            get_transform("unknown_transform", {})

        assert "unknown_transform" in str(exc_info.value)
        assert "available_types" in exc_info.value.context

    def test_register_duplicate_raises(self):
        """Registering duplicate transform should raise."""
        clear_registry()

        def dummy_factory(config):
            return lambda batch, cfg: batch

        register_transform("test_transform", dummy_factory)

        with pytest.raises(TransformError) as exc_info:
            register_transform("test_transform", dummy_factory)

        assert "already registered" in str(exc_info.value)

    def test_clear_registry(self):
        """clear_registry should remove all transforms."""
        clear_registry()
        assert list_transform_types() == []


# ============================================================================
# RenameColumnsTransform Tests
# ============================================================================


class TestRenameColumnsTransform:
    """Tests for rename_columns transform."""

    def test_rename_single_column(self, sample_batch):
        """Should rename a single column."""
        transform = RenameColumnsTransform({"mapping": {"name": "full_name"}})
        result = transform.apply(sample_batch)

        assert result.columns == ["id", "full_name", "age", "created_at"]
        assert result.rows[0][1] == "Alice"

    def test_rename_multiple_columns(self, sample_batch):
        """Should rename multiple columns."""
        transform = RenameColumnsTransform({
            "mapping": {"name": "full_name", "age": "years"}
        })
        result = transform.apply(sample_batch)

        assert result.columns == ["id", "full_name", "years", "created_at"]

    def test_preserves_metadata(self, sample_batch):
        """Should preserve batch metadata."""
        transform = RenameColumnsTransform({"mapping": {"name": "full_name"}})
        result = transform.apply(sample_batch)

        assert result.metadata == {"source": "test"}

    def test_missing_column_raises(self, sample_batch):
        """Should raise when mapping references non-existent column."""
        transform = RenameColumnsTransform({"mapping": {"nonexistent": "new"}})

        with pytest.raises(TransformError) as exc_info:
            transform.apply(sample_batch)

        assert "nonexistent" in str(exc_info.value)
        assert "missing_columns" in exc_info.value.context

    def test_missing_mapping_config_raises(self):
        """Should raise when mapping not provided."""
        with pytest.raises(TransformError) as exc_info:
            RenameColumnsTransform({})

        assert "mapping" in str(exc_info.value)

    def test_invalid_mapping_type_raises(self):
        """Should raise when mapping is not a dict."""
        with pytest.raises(TransformError):
            RenameColumnsTransform({"mapping": ["a", "b"]})

    def test_empty_batch(self, empty_batch):
        """Should handle empty batch."""
        transform = RenameColumnsTransform({"mapping": {"name": "full_name"}})
        result = transform.apply(empty_batch)

        assert result.columns == ["id", "full_name"]
        assert result.rows == []


# ============================================================================
# CastColumnsTransform Tests
# ============================================================================


class TestCastColumnsTransform:
    """Tests for cast transform."""

    def test_cast_to_int(self, sample_batch):
        """Should cast string to int."""
        transform = CastColumnsTransform({"columns": {"age": "int"}})
        result = transform.apply(sample_batch)

        assert result.rows[0][2] == 25
        assert isinstance(result.rows[0][2], int)

    def test_cast_to_float(self):
        """Should cast string to float."""
        batch = ArrowBatch.from_rows(
            columns=["price"],
            rows=[["19.99"], ["5.50"]],
            metadata={},
        )
        transform = CastColumnsTransform({"columns": {"price": "float"}})
        result = transform.apply(batch)

        assert result.rows[0][0] == 19.99
        assert isinstance(result.rows[0][0], float)

    def test_cast_to_str(self):
        """Should cast int to string."""
        batch = ArrowBatch.from_rows(
            columns=["code"],
            rows=[[123], [456]],
            metadata={},
        )
        transform = CastColumnsTransform({"columns": {"code": "str"}})
        result = transform.apply(batch)

        assert result.rows[0][0] == "123"
        assert isinstance(result.rows[0][0], str)

    def test_cast_to_datetime(self, sample_batch):
        """Should cast ISO string to datetime."""
        transform = CastColumnsTransform({"columns": {"created_at": "datetime"}})
        result = transform.apply(sample_batch)

        assert isinstance(result.rows[0][3], datetime)
        assert result.rows[0][3].year == 2024
        assert result.rows[0][3].month == 1

    def test_cast_multiple_columns(self, sample_batch):
        """Should cast multiple columns."""
        transform = CastColumnsTransform({
            "columns": {"age": "int", "created_at": "datetime"}
        })
        result = transform.apply(sample_batch)

        assert isinstance(result.rows[0][2], int)
        assert isinstance(result.rows[0][3], datetime)

    def test_preserves_metadata(self, sample_batch):
        """Should preserve batch metadata."""
        transform = CastColumnsTransform({"columns": {"age": "int"}})
        result = transform.apply(sample_batch)

        assert result.metadata == {"source": "test"}

    def test_handles_none_values(self):
        """Should preserve None values without casting."""
        batch = ArrowBatch.from_rows(
            columns=["value"],
            rows=[["10"], [None], ["20"]],
            metadata={},
        )
        transform = CastColumnsTransform({"columns": {"value": "int"}})
        result = transform.apply(batch)

        assert result.rows[0][0] == 10
        assert result.rows[1][0] is None
        assert result.rows[2][0] == 20

    def test_invalid_cast_raises(self):
        """Should raise on invalid conversion."""
        batch = ArrowBatch.from_rows(
            columns=["value"],
            rows=[["not_a_number"]],
            metadata={},
        )
        transform = CastColumnsTransform({"columns": {"value": "int"}})

        with pytest.raises(TransformError) as exc_info:
            transform.apply(batch)

        assert "Cast failed" in str(exc_info.value)

    def test_unsupported_type_raises(self):
        """Should raise for unsupported type."""
        with pytest.raises(TransformError) as exc_info:
            CastColumnsTransform({"columns": {"value": "unknown_type"}})

        assert "Unsupported types" in str(exc_info.value)

    def test_missing_column_raises(self, sample_batch):
        """Should raise for non-existent column."""
        transform = CastColumnsTransform({"columns": {"nonexistent": "int"}})

        with pytest.raises(TransformError) as exc_info:
            transform.apply(sample_batch)

        assert "nonexistent" in str(exc_info.value)

    def test_missing_columns_config_raises(self):
        """Should raise when columns not provided."""
        with pytest.raises(TransformError):
            CastColumnsTransform({})


# ============================================================================
# AddColumnTransform Tests
# ============================================================================


class TestAddColumnTransform:
    """Tests for add_column transform."""

    def test_add_constant_string(self, sample_batch):
        """Should add column with constant string value."""
        transform = AddColumnTransform({"name": "status", "value": "active"})
        result = transform.apply(sample_batch)

        assert "status" in result.columns
        assert all(row[-1] == "active" for row in result.rows)

    def test_add_constant_number(self, sample_batch):
        """Should add column with constant number value."""
        transform = AddColumnTransform({"name": "score", "value": 100})
        result = transform.apply(sample_batch)

        assert "score" in result.columns
        assert all(row[-1] == 100 for row in result.rows)

    def test_add_constant_none(self, sample_batch):
        """Should add column with None value."""
        transform = AddColumnTransform({"name": "nullable", "value": None})
        result = transform.apply(sample_batch)

        assert all(row[-1] is None for row in result.rows)

    def test_preserves_metadata(self, sample_batch):
        """Should preserve batch metadata."""
        transform = AddColumnTransform({"name": "new_col", "value": "x"})
        result = transform.apply(sample_batch)

        assert result.metadata == {"source": "test"}

    def test_preserves_existing_columns(self, sample_batch):
        """Should preserve existing column order."""
        transform = AddColumnTransform({"name": "new_col", "value": "x"})
        result = transform.apply(sample_batch)

        assert result.columns[:4] == ["id", "name", "age", "created_at"]
        assert result.columns[4] == "new_col"

    def test_duplicate_column_raises(self, sample_batch):
        """Should raise when adding existing column."""
        transform = AddColumnTransform({"name": "name", "value": "x"})

        with pytest.raises(TransformError) as exc_info:
            transform.apply(sample_batch)

        assert "already exists" in str(exc_info.value)

    def test_missing_name_config_raises(self):
        """Should raise when name not provided."""
        with pytest.raises(TransformError):
            AddColumnTransform({"value": "x"})

    def test_missing_value_config_raises(self):
        """Should raise when value not provided."""
        with pytest.raises(TransformError):
            AddColumnTransform({"name": "col"})

    def test_template_recipe_name(self, sample_batch):
        """Should render recipe.name template when context provided."""
        transform = AddColumnTransform({
            "name": "recipe",
            "value": "{{ recipe.name }}",
            "context": {"recipe": {"name": "my_recipe"}},
        })
        result = transform.apply(sample_batch)

        assert all(row[-1] == "my_recipe" for row in result.rows)

    def test_template_without_context_returns_original(self, sample_batch):
        """Template without context should return original string."""
        transform = AddColumnTransform({
            "name": "template",
            "value": "{{ recipe.name }}",
        })
        result = transform.apply(sample_batch)

        assert all(row[-1] == "{{ recipe.name }}" for row in result.rows)

    def test_empty_batch(self, empty_batch):
        """Should handle empty batch."""
        transform = AddColumnTransform({"name": "new_col", "value": "x"})
        result = transform.apply(empty_batch)

        assert "new_col" in result.columns
        assert result.rows == []


# ============================================================================
# TransformPipeline Tests
# ============================================================================


class TestTransformPipeline:
    """Tests for TransformPipeline executor."""

    def test_empty_pipeline(self, sample_batch):
        """Empty pipeline should return batch unchanged."""
        config = TransformConfig(steps=[])
        pipeline = TransformPipeline(config)
        result = pipeline.apply(sample_batch)

        assert result.columns == sample_batch.columns
        assert result.rows == sample_batch.rows

    def test_single_step(self, sample_batch):
        """Pipeline with single step should apply it."""
        config = TransformConfig(steps=[
            TransformStep(type="rename_columns", mapping={"name": "full_name"})
        ])
        pipeline = TransformPipeline(config)
        result = pipeline.apply(sample_batch)

        assert "full_name" in result.columns
        assert "name" not in result.columns

    def test_multiple_steps_sequential(self, sample_batch):
        """Pipeline should apply steps in order."""
        config = TransformConfig(steps=[
            TransformStep(type="rename_columns", mapping={"name": "full_name"}),
            TransformStep(type="cast", columns={"age": "int"}),
            TransformStep(type="add_column", name="status", value="active"),
        ])
        pipeline = TransformPipeline(config)
        result = pipeline.apply(sample_batch)

        assert "full_name" in result.columns
        assert isinstance(result.rows[0][2], int)
        assert result.rows[0][-1] == "active"

    def test_preserves_metadata_through_pipeline(self, sample_batch):
        """Metadata should be preserved through multiple steps."""
        config = TransformConfig(steps=[
            TransformStep(type="rename_columns", mapping={"name": "full_name"}),
            TransformStep(type="add_column", name="status", value="active"),
        ])
        pipeline = TransformPipeline(config)
        result = pipeline.apply(sample_batch)

        assert result.metadata == {"source": "test"}

    def test_error_includes_step_context(self, sample_batch):
        """Transform error should include step index."""
        config = TransformConfig(steps=[
            TransformStep(type="rename_columns", mapping={"name": "full_name"}),
            TransformStep(type="cast", columns={"nonexistent": "int"}),
        ])
        pipeline = TransformPipeline(config)

        with pytest.raises(TransformError) as exc_info:
            pipeline.apply(sample_batch)

        # Error should mention the failing column
        assert "nonexistent" in str(exc_info.value)

    def test_unknown_transform_type_raises(self, sample_batch):
        """Unknown transform type should raise TransformError."""
        config = TransformConfig(steps=[
            TransformStep(type="unknown_transform_xyz")
        ])
        pipeline = TransformPipeline(config)

        with pytest.raises(TransformError) as exc_info:
            pipeline.apply(sample_batch)

        assert "unknown_transform_xyz" in str(exc_info.value)

    def test_step_output_feeds_next_step(self):
        """Each step should receive output of previous step."""
        batch = ArrowBatch.from_rows(
            columns=["old_name"],
            rows=[["value"]],
            metadata={},
        )
        config = TransformConfig(steps=[
            TransformStep(type="rename_columns", mapping={"old_name": "new_name"}),
            # This would fail if it still had 'old_name'
            TransformStep(type="rename_columns", mapping={"new_name": "final_name"}),
        ])
        pipeline = TransformPipeline(config)
        result = pipeline.apply(batch)

        assert result.columns == ["final_name"]


# ============================================================================
# Integration Tests
# ============================================================================


class TestTransformIntegration:
    """Integration tests for complete transform workflows."""

    def test_realistic_pipeline(self):
        """Test a realistic data transformation pipeline."""
        # Raw data from source
        batch = ArrowBatch.from_rows(
            columns=["user_id", "user_name", "age_str", "signup_date"],
            rows=[
                ["1", "alice", "25", "2024-01-15T10:30:00"],
                ["2", "bob", "30", "2024-02-20T14:45:00"],
            ],
            metadata={"batch_id": "001", "source": "postgres"},
        )

        # Pipeline: rename -> cast -> add metadata column
        config = TransformConfig(steps=[
            TransformStep(
                type="rename_columns",
                mapping={
                    "user_id": "id",
                    "user_name": "name",
                    "age_str": "age",
                    "signup_date": "created_at",
                }
            ),
            TransformStep(
                type="cast",
                columns={
                    "id": "int",
                    "age": "int",
                    "created_at": "datetime",
                }
            ),
            TransformStep(
                type="add_column",
                name="processed",
                value=True,
            ),
        ])

        pipeline = TransformPipeline(config)
        result = pipeline.apply(batch)

        # Verify columns renamed
        assert result.columns == ["id", "name", "age", "created_at", "processed"]

        # Verify types cast
        assert result.rows[0][0] == 1
        assert isinstance(result.rows[0][0], int)
        assert result.rows[0][2] == 25
        assert isinstance(result.rows[0][3], datetime)

        # Verify column added
        assert result.rows[0][4] is True
        assert result.rows[1][4] is True

        # Verify metadata preserved
        assert result.metadata["batch_id"] == "001"
        assert result.metadata["source"] == "postgres"

