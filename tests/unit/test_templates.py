"""Tests for template rendering."""

import os
import pytest

from dataloader.core.exceptions import RecipeError
from dataloader.models.templates import render_templates


class TestTemplateRendering:
    """Tests for template rendering functionality."""

    def test_recipe_name_template(self):
        """Test {{ recipe.name }} template."""
        recipe_dict = {
            "name": "my_recipe",
            "source": {
                "type": "postgres",
                "host": "{{ recipe.name }}.example.com",
            },
        }
        result = render_templates(recipe_dict)
        assert result["source"]["host"] == "my_recipe.example.com"

    def test_env_var_template(self, monkeypatch):
        """Test {{ env_var('KEY') }} template."""
        monkeypatch.setenv("TEST_HOST", "test.example.com")
        recipe_dict = {
            "name": "test",
            "source": {
                "type": "postgres",
                "host": "{{ env_var('TEST_HOST') }}",
            },
        }
        result = render_templates(recipe_dict)
        assert result["source"]["host"] == "test.example.com"

    def test_env_var_missing(self):
        """Test that missing env_var raises error."""
        recipe_dict = {
            "name": "test",
            "source": {
                "type": "postgres",
                "host": "{{ env_var('MISSING_VAR') }}",
            },
        }
        with pytest.raises(RecipeError) as exc_info:
            render_templates(recipe_dict)
        assert "MISSING_VAR" in str(exc_info.value)

    def test_var_template(self):
        """Test {{ var('KEY') }} template with CLI vars."""
        recipe_dict = {
            "name": "test",
            "source": {
                "type": "postgres",
                "host": "{{ var('CLI_HOST') }}",
            },
        }
        cli_vars = {"CLI_HOST": "cli.example.com"}
        result = render_templates(recipe_dict, cli_vars)
        assert result["source"]["host"] == "cli.example.com"

    def test_var_missing(self):
        """Test that missing var raises error."""
        recipe_dict = {
            "name": "test",
            "source": {
                "type": "postgres",
                "host": "{{ var('MISSING_VAR') }}",
            },
        }
        with pytest.raises(RecipeError) as exc_info:
            render_templates(recipe_dict, {})
        assert "MISSING_VAR" in str(exc_info.value)

    def test_nested_templates(self):
        """Test templates in nested structures."""
        recipe_dict = {
            "name": "test_recipe",
            "source": {
                "type": "postgres",
                "host": "{{ recipe.name }}.db.example.com",
                "database": "{{ var('DB_NAME') }}",
            },
            "transform": {
                "steps": [
                    {
                        "type": "add_column",
                        "name": "_source",
                        "value": "{{ recipe.name }}",
                    }
                ],
            },
        }
        cli_vars = {"DB_NAME": "mydb"}
        result = render_templates(recipe_dict, cli_vars)
        assert result["source"]["host"] == "test_recipe.db.example.com"
        assert result["source"]["database"] == "mydb"
        assert result["transform"]["steps"][0]["value"] == "test_recipe"

    def test_multiple_templates_in_string(self):
        """Test multiple templates in a single string."""
        recipe_dict = {
            "name": "test",
            "source": {
                "type": "postgres",
                "host": "{{ env_var('HOST') }}",
                "database": "{{ var('DB') }}",
            },
        }
        import os

        os.environ["HOST"] = "example.com"
        cli_vars = {"DB": "testdb"}
        result = render_templates(recipe_dict, cli_vars)
        assert result["source"]["host"] == "example.com"
        assert result["source"]["database"] == "testdb"

    def test_no_templates(self):
        """Test recipe with no templates."""
        recipe_dict = {
            "name": "test",
            "source": {
                "type": "postgres",
                "host": "localhost",
            },
        }
        result = render_templates(recipe_dict)
        assert result["source"]["host"] == "localhost"

    def test_list_templates(self):
        """Test templates in list values."""
        recipe_dict = {
            "name": "test",
            "transform": {
                "steps": [
                    {
                        "type": "add_column",
                        "name": "_source",
                        "value": "{{ recipe.name }}",
                    }
                ],
            },
        }
        result = render_templates(recipe_dict)
        assert result["transform"]["steps"][0]["value"] == "test"

