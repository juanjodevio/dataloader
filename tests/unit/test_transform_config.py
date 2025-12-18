"""Tests for TransformConfig model."""

from dataloader.models.transform_config import TransformConfig, TransformStep


class TestTransformStep:
    """Tests for TransformStep."""

    def test_basic_transform_step(self):
        """Test basic transform step."""
        step = TransformStep(type="rename_columns", mapping={"old": "new"})
        assert step.type == "rename_columns"
        assert step.mapping == {"old": "new"}

    def test_transform_step_extra_fields(self):
        """Test transform step with extra fields (flexible config)."""
        step = TransformStep(
            type="add_column",
            name="_loaded_at",
            value="now()",
            some_other_field="allowed",
        )
        assert step.type == "add_column"
        assert step.name == "_loaded_at"
        assert step.value == "now()"
        assert step.some_other_field == "allowed"


class TestTransformConfig:
    """Tests for TransformConfig."""

    def test_empty_steps(self):
        """Test transform config with empty steps."""
        config = TransformConfig(steps=[])
        assert config.steps == []

    def test_multiple_steps(self):
        """Test transform config with multiple steps."""
        steps = [
            TransformStep(type="rename_columns", mapping={"old": "new"}),
            TransformStep(type="add_column", name="_loaded_at", value="now()"),
        ]
        config = TransformConfig(steps=steps)
        assert len(config.steps) == 2
        assert config.steps[0].type == "rename_columns"
        assert config.steps[1].type == "add_column"

