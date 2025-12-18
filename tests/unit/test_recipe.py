"""Tests for Recipe model."""

import pytest
from pydantic import ValidationError

from dataloader.models.destination_config import DestinationConfig
from dataloader.models.recipe import Recipe
from dataloader.models.runtime_config import RuntimeConfig
from dataloader.models.source_config import SourceConfig
from dataloader.models.transform_config import TransformConfig


class TestRecipe:
    """Tests for Recipe model."""

    def test_minimal_recipe(self):
        """Test minimal valid recipe."""
        recipe = Recipe(
            name="test_recipe",
            source=SourceConfig(
                type="postgres",
                host="localhost",
                database="testdb",
                user="testuser",
                password="testpass",
                table="public.test",
            ),
            transform=TransformConfig(steps=[]),
            destination=DestinationConfig(
                type="redshift",
                host="redshift.example.com",
                database="dw",
                user="dwuser",
                password="dwpass",
                table="dw.test",
            ),
        )
        assert recipe.name == "test_recipe"
        assert recipe.extends is None

    def test_recipe_with_extends(self):
        """Test recipe with extends field."""
        recipe = Recipe(
            name="child_recipe",
            extends="base_recipe.yaml",
            source=SourceConfig(
                type="postgres",
                host="localhost",
                database="testdb",
                user="testuser",
                password="testpass",
                table="public.test",
            ),
            transform=TransformConfig(steps=[]),
            destination=DestinationConfig(
                type="redshift",
                host="redshift.example.com",
                database="dw",
                user="dwuser",
                password="dwpass",
                table="dw.test",
            ),
        )
        assert recipe.extends == "base_recipe.yaml"

    def test_recipe_missing_name(self):
        """Test that name is required."""
        with pytest.raises(ValidationError):
            Recipe(
                source=SourceConfig(
                    type="postgres",
                    host="localhost",
                    database="testdb",
                    user="testuser",
                    password="testpass",
                    table="public.test",
                ),
                transform=TransformConfig(steps=[]),
                destination=DestinationConfig(
                    type="redshift",
                    host="redshift.example.com",
                    database="dw",
                    user="dwuser",
                    password="dwpass",
                    table="dw.test",
                ),
            )

    def test_recipe_with_runtime(self):
        """Test recipe with custom runtime config."""
        recipe = Recipe(
            name="test_recipe",
            source=SourceConfig(
                type="postgres",
                host="localhost",
                database="testdb",
                user="testuser",
                password="testpass",
                table="public.test",
            ),
            transform=TransformConfig(steps=[]),
            destination=DestinationConfig(
                type="redshift",
                host="redshift.example.com",
                database="dw",
                user="dwuser",
                password="dwpass",
                table="dw.test",
            ),
            runtime=RuntimeConfig(batch_size=5000, max_retries=3),
        )
        assert recipe.runtime.batch_size == 5000
        assert recipe.runtime.max_retries == 3

    def test_recipe_from_dict(self):
        """Test creating recipe from dictionary."""
        data = {
            "name": "test_recipe",
            "source": {
                "type": "postgres",
                "host": "localhost",
                "database": "testdb",
                "user": "testuser",
                "password": "testpass",
                "table": "public.test",
            },
            "transform": {"steps": []},
            "destination": {
                "type": "redshift",
                "host": "redshift.example.com",
                "database": "dw",
                "user": "dwuser",
                "password": "dwpass",
                "table": "dw.test",
            },
        }
        recipe = Recipe.from_dict(data)
        assert recipe.name == "test_recipe"
        assert recipe.source.type == "postgres"

