"""Tests for recipe loader."""

from pathlib import Path

import pytest
import yaml

from dataloader.core.exceptions import RecipeError
from dataloader.models.loader import load_recipe


class TestRecipeLoader:
    """Tests for recipe loading functionality."""

    def test_load_simple_recipe(self, recipe_dir):
        """Test loading a simple recipe from YAML."""
        recipe_file = recipe_dir / "simple.yaml"
        recipe_file.write_text(
            """
name: simple_recipe
source:
  type: postgres
  host: localhost
  database: testdb
  user: testuser
  password: testpass
  table: public.test
transform:
  steps: []
destination:
  type: redshift
  host: redshift.example.com
  database: dw
  user: dwuser
  password: dwpass
  table: dw.test
"""
        )
        recipe = load_recipe(str(recipe_file))
        assert recipe.name == "simple_recipe"
        assert recipe.source.type == "postgres"
        assert recipe.destination.type == "redshift"

    def test_load_recipe_with_inheritance(self, recipe_dir):
        """Test loading recipe with inheritance."""
        base_file = recipe_dir / "base.yaml"
        base_file.write_text(
            """
name: base_recipe
transform:
  steps:
    - type: add_column
      name: _loaded_at
      value: now()
runtime:
  batch_size: 20000
"""
        )

        child_file = recipe_dir / "child.yaml"
        child_file.write_text(
            """
extends: base.yaml
name: child_recipe
source:
  type: postgres
  host: localhost
  database: testdb
  user: testuser
  password: testpass
  table: public.test
transform:
  steps:
    - type: rename_columns
      mapping:
        old: new
destination:
  type: redshift
  host: redshift.example.com
  database: dw
  user: dwuser
  password: dwpass
  table: dw.test
runtime:
  batch_size: 5000
"""
        )

        recipe = load_recipe(str(child_file))
        assert recipe.name == "child_recipe"
        assert len(recipe.transform.steps) == 2
        assert recipe.transform.steps[0].type == "add_column"
        assert recipe.transform.steps[1].type == "rename_columns"
        assert recipe.runtime.batch_size == 5000

    def test_load_recipe_multi_level_inheritance(self, recipe_dir):
        """Test multi-level inheritance."""
        grandparent_file = recipe_dir / "grandparent.yaml"
        grandparent_file.write_text(
            """
name: grandparent
runtime:
  batch_size: 10000
"""
        )

        parent_file = recipe_dir / "parent.yaml"
        parent_file.write_text(
            """
extends: grandparent.yaml
name: parent
runtime:
  batch_size: 20000
"""
        )

        child_file = recipe_dir / "child.yaml"
        child_file.write_text(
            """
extends: parent.yaml
name: child
source:
  type: postgres
  host: localhost
  database: testdb
  user: testuser
  password: testpass
  table: public.test
transform:
  steps: []
destination:
  type: redshift
  host: redshift.example.com
  database: dw
  user: dwuser
  password: dwpass
  table: dw.test
runtime:
  batch_size: 5000
"""
        )

        recipe = load_recipe(str(child_file))
        assert recipe.name == "child"
        assert recipe.runtime.batch_size == 5000

    def test_cycle_detection(self, recipe_dir):
        """Test that cycles in inheritance are detected."""
        file_a = recipe_dir / "a.yaml"
        file_a.write_text(
            """
extends: b.yaml
name: recipe_a
source:
  type: postgres
  host: localhost
  database: testdb
  user: testuser
  password: testpass
  table: public.test
transform:
  steps: []
destination:
  type: redshift
  host: redshift.example.com
  database: dw
  user: dwuser
  password: dwpass
  table: dw.test
"""
        )

        file_b = recipe_dir / "b.yaml"
        file_b.write_text(
            """
extends: a.yaml
name: recipe_b
source:
  type: postgres
  host: localhost
  database: testdb
  user: testuser
  password: testpass
  table: public.test
transform:
  steps: []
destination:
  type: redshift
  host: redshift.example.com
  database: dw
  user: dwuser
  password: dwpass
  table: dw.test
"""
        )

        with pytest.raises(RecipeError) as exc_info:
            load_recipe(str(file_a))
        assert "Cycle detected" in str(exc_info.value)

    def test_file_not_found(self):
        """Test error when recipe file doesn't exist."""
        with pytest.raises(RecipeError) as exc_info:
            load_recipe("nonexistent.yaml")
        assert "not found" in str(exc_info.value).lower()

    def test_invalid_yaml(self, recipe_dir):
        """Test error when YAML is invalid."""
        recipe_file = recipe_dir / "invalid.yaml"
        recipe_file.write_text("invalid: yaml: content: [unclosed")

        with pytest.raises(RecipeError) as exc_info:
            load_recipe(str(recipe_file))
        assert "Invalid YAML" in str(exc_info.value)

    def test_delete_semantics(self, recipe_dir):
        """Test delete semantics in recipe."""
        base_file = recipe_dir / "base.yaml"
        base_file.write_text(
            """
name: base
transform:
  steps:
    - type: add_column
      name: _loaded_at
      value: now()
"""
        )

        child_file = recipe_dir / "child.yaml"
        child_file.write_text(
            """
extends: base.yaml
name: child
source:
  type: postgres
  host: localhost
  database: testdb
  user: testuser
  password: testpass
  table: public.test
transform:
  steps: []
destination:
  type: redshift
  host: redshift.example.com
  database: dw
  user: dwuser
  password: dwpass
  table: dw.test
delete:
  - transform.steps
"""
        )

        recipe = load_recipe(str(child_file))
        assert len(recipe.transform.steps) == 0

    def test_template_rendering_in_loader(self, recipe_dir, monkeypatch):
        """Test that templates are rendered during loading."""
        monkeypatch.setenv("TEST_HOST", "template.example.com")
        recipe_file = recipe_dir / "template.yaml"
        recipe_file.write_text(
            """
name: template_recipe
source:
  type: postgres
  host: "{{ env_var('TEST_HOST') }}"
  database: testdb
  user: testuser
  password: testpass
  table: public.test
transform:
  steps: []
destination:
  type: redshift
  host: redshift.example.com
  database: dw
  user: dwuser
  password: dwpass
  table: dw.test
"""
        )

        recipe = load_recipe(str(recipe_file))
        assert recipe.source.host == "template.example.com"

    def test_cli_vars_in_loader(self, recipe_dir):
        """Test that CLI vars are passed to template rendering."""
        recipe_file = recipe_dir / "cli_template.yaml"
        recipe_file.write_text(
            """
name: cli_recipe
source:
  type: postgres
  host: localhost
  database: "{{ var('DB_NAME') }}"
  user: testuser
  password: testpass
  table: public.test
transform:
  steps: []
destination:
  type: redshift
  host: redshift.example.com
  database: dw
  user: dwuser
  password: dwpass
  table: dw.test
"""
        )

        recipe = load_recipe(str(recipe_file), cli_vars={"DB_NAME": "cli_database"})
        assert recipe.source.database == "cli_database"
