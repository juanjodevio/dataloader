"""Tests for deep merge algorithm."""

from dataloader.models.merger import apply_delete_semantics, merge_recipes


class TestMergeRecipes:
    """Tests for merge_recipes function."""

    def test_scalar_override(self):
        """Test that child scalars override parent."""
        parent = {"runtime": {"batch_size": 20000}}
        child = {"runtime": {"batch_size": 5000}}
        result = merge_recipes(parent, child)
        assert result["runtime"]["batch_size"] == 5000

    def test_dict_deep_merge(self):
        """Test deep merge of dictionaries."""
        parent = {
            "source": {
                "type": "postgres",
                "host": "localhost",
                "port": 5432,
            }
        }
        child = {
            "source": {
                "port": 5433,
                "database": "testdb",
            }
        }
        result = merge_recipes(parent, child)
        assert result["source"]["type"] == "postgres"
        assert result["source"]["host"] == "localhost"
        assert result["source"]["port"] == 5433
        assert result["source"]["database"] == "testdb"

    def test_transform_steps_concatenation(self):
        """Test that transform.steps are concatenated."""
        parent = {
            "transform": {
                "steps": [
                    {"type": "add_column", "name": "_loaded_at"},
                    {"type": "add_column", "name": "_source"},
                ]
            }
        }
        child = {
            "transform": {
                "steps": [
                    {"type": "rename_columns", "mapping": {"old": "new"}},
                ]
            }
        }
        result = merge_recipes(parent, child)
        assert len(result["transform"]["steps"]) == 3
        assert result["transform"]["steps"][0]["type"] == "add_column"
        assert result["transform"]["steps"][1]["type"] == "add_column"
        assert result["transform"]["steps"][2]["type"] == "rename_columns"

    def test_missing_keys_inherited(self):
        """Test that missing keys are inherited from parent."""
        parent = {
            "runtime": {"batch_size": 20000},
            "source": {"type": "postgres"},
        }
        child = {"source": {"host": "localhost"}}
        result = merge_recipes(parent, child)
        assert result["runtime"]["batch_size"] == 20000
        assert result["source"]["type"] == "postgres"
        assert result["source"]["host"] == "localhost"

    def test_delete_preserved(self):
        """Test that delete key is preserved during merge."""
        parent = {"runtime": {"batch_size": 20000}}
        child = {"delete": ["transform.steps"]}
        result = merge_recipes(parent, child)
        assert "delete" in result
        assert result["delete"] == ["transform.steps"]

    def test_nested_merge(self):
        """Test nested dictionary merging."""
        parent = {
            "source": {
                "type": "postgres",
                "connection": {
                    "host": "localhost",
                    "port": 5432,
                },
            }
        }
        child = {
            "source": {
                "connection": {
                    "port": 5433,
                    "database": "testdb",
                },
            }
        }
        result = merge_recipes(parent, child)
        assert result["source"]["type"] == "postgres"
        assert result["source"]["connection"]["host"] == "localhost"
        assert result["source"]["connection"]["port"] == 5433
        assert result["source"]["connection"]["database"] == "testdb"


class TestDeleteSemantics:
    """Tests for delete semantics."""

    def test_delete_simple_path(self):
        """Test deleting a simple path."""
        recipe = {
            "source": {
                "host": "localhost",
                "port": 5432,
            },
            "runtime": {"batch_size": 20000},
        }
        result = apply_delete_semantics(recipe, ["source.port"])
        assert "port" not in result["source"]
        assert result["source"]["host"] == "localhost"
        assert result["runtime"]["batch_size"] == 20000

    def test_delete_nested_path(self):
        """Test deleting a nested path."""
        recipe = {
            "transform": {
                "steps": [
                    {"type": "add_column", "name": "_loaded_at"},
                    {"type": "rename_columns", "mapping": {}},
                ]
            }
        }
        result = apply_delete_semantics(recipe, ["transform.steps"])
        assert "steps" not in result["transform"]

    def test_delete_multiple_paths(self):
        """Test deleting multiple paths."""
        recipe = {
            "source": {"host": "localhost", "port": 5432},
            "destination": {"merge_keys": ["id"]},
            "runtime": {"batch_size": 20000},
        }
        result = apply_delete_semantics(recipe, ["source.port", "destination.merge_keys"])
        assert "port" not in result["source"]
        assert "merge_keys" not in result["destination"]
        assert result["source"]["host"] == "localhost"
        assert result["runtime"]["batch_size"] == 20000

    def test_delete_nonexistent_path(self):
        """Test deleting a path that doesn't exist (should not error)."""
        recipe = {"source": {"host": "localhost"}}
        result = apply_delete_semantics(recipe, ["source.port", "nonexistent.key"])
        assert "port" not in result["source"]
        assert result["source"]["host"] == "localhost"

    def test_delete_empty_list(self):
        """Test delete with empty list."""
        recipe = {"source": {"host": "localhost"}}
        result = apply_delete_semantics(recipe, [])
        assert result == recipe

