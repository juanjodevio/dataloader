"""Integration tests for recipe loading with real YAML files."""

import os
import pytest
from pathlib import Path

from dataloader.models import Recipe


@pytest.mark.integration
class TestRecipeLoadingIntegration:
    """Integration tests for recipe loading."""

    def test_load_example_recipe(self):
        """Test loading an example recipe from examples/recipes."""
        recipe_path = Path(__file__).parent.parent.parent / "examples" / "recipes" / "test_recipe.yaml"
        recipe = Recipe.from_yaml(str(recipe_path))
        assert recipe.name == "test_recipe"
        assert recipe.source.type == "postgres"
        assert recipe.destination.type == "redshift"
        assert len(recipe.transform.steps) == 1

    def test_load_recipe_with_inheritance(self):
        """Test loading recipe with inheritance from examples."""
        base_path = Path(__file__).parent.parent.parent / "examples" / "recipes" / "base_recipe.yaml"
        child_path = Path(__file__).parent.parent.parent / "examples" / "recipes" / "child_recipe.yaml"

        # Set environment variable for template rendering
        os.environ["SOURCE_HOST"] = "localhost"

        recipe = Recipe.from_yaml(str(child_path))
        assert recipe.name == "child_recipe"
        assert len(recipe.transform.steps) == 2
        assert recipe.runtime.batch_size == 5000
        assert recipe.source.host == "localhost"

    def test_load_recipe_with_templates(self, monkeypatch):
        """Test loading recipe with template rendering."""
        monkeypatch.setenv("SOURCE_HOST", "template.example.com")
        monkeypatch.setenv("SOURCE_USER", "template_user")
        monkeypatch.setenv("SOURCE_PASSWORD", "template_pass")
        monkeypatch.setenv("DEST_HOST", "dest.example.com")
        monkeypatch.setenv("DEST_USER", "dest_user")
        monkeypatch.setenv("DEST_PASSWORD", "dest_pass")

        recipe_path = Path(__file__).parent.parent.parent / "examples" / "recipes" / "test_credentials.yaml"
        recipe = Recipe.from_yaml(
            str(recipe_path),
            cli_vars={"SOURCE_DB": "cli_db", "DEST_DB": "cli_dest_db"},
        )
        assert recipe.source.host == "template.example.com"
        assert recipe.source.database == "cli_db"
        assert recipe.destination.database == "cli_dest_db"

    def test_delete_semantics_integration(self):
        """Test delete semantics with real recipe files."""
        recipe_path = Path(__file__).parent.parent.parent / "examples" / "recipes" / "delete_test.yaml"
        recipe = Recipe.from_yaml(str(recipe_path))
        assert len(recipe.transform.steps) == 0

