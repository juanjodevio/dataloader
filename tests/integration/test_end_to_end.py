"""End-to-end integration tests for complete data loading pipelines."""

import csv
import tempfile
from pathlib import Path

import duckdb
import pytest

from dataloader import from_yaml, run_recipe, run_recipe_from_yaml
from dataloader.core.state_backend import LocalStateBackend


@pytest.mark.integration
class TestEndToEndPipelines:
    """End-to-end tests for complete data loading pipelines."""

    def test_csv_to_duckdb_pipeline(self, temp_dir):
        """Test complete pipeline: CSV → DuckDB."""
        # Create test CSV file
        csv_path = temp_dir / "test_data.csv"
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "name", "value"])
            writer.writerow([1, "Alice", 10.5])
            writer.writerow([2, "Bob", 20.3])
            writer.writerow([3, "Charlie", 30.7])

        # Create recipe YAML
        recipe_path = temp_dir / "recipe.yaml"
        recipe_content = f"""
name: csv_to_duckdb

source:
  type: csv
  path: "{csv_path}"

transform:
  steps:
    - type: add_column
      name: _loaded_at
      value: "test"

destination:
  type: duckdb
  database: "{temp_dir / 'output.duckdb'}"
  table: test_table
  write_mode: overwrite
"""
        recipe_path.write_text(recipe_content)

        # Execute recipe
        state_backend = LocalStateBackend(temp_dir / ".state")
        run_recipe_from_yaml(str(recipe_path), state_dir=str(temp_dir / ".state"))

        # Verify data in DuckDB
        conn = duckdb.connect(str(temp_dir / "output.duckdb"))
        result = conn.execute("SELECT * FROM test_table ORDER BY id").fetchall()
        conn.close()

        assert len(result) == 3
        assert result[0] == (1, "Alice", 10.5, "test")
        assert result[1] == (2, "Bob", 20.3, "test")
        assert result[2] == (3, "Charlie", 30.7, "test")

    def test_csv_to_duckdb_with_transforms(self, temp_dir):
        """Test CSV → DuckDB pipeline with column renaming."""
        # Create test CSV file
        csv_path = temp_dir / "test_data.csv"
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "fname", "lname"])
            writer.writerow([1, "Alice", "Smith"])
            writer.writerow([2, "Bob", "Jones"])

        # Create recipe YAML
        recipe_path = temp_dir / "recipe.yaml"
        recipe_content = f"""
name: csv_transform

source:
  type: csv
  path: "{csv_path}"

transform:
  steps:
    - type: rename_columns
      mapping:
        fname: first_name
        lname: last_name

destination:
  type: duckdb
  database: "{temp_dir / 'output.duckdb'}"
  table: test_table
  write_mode: overwrite
"""
        recipe_path.write_text(recipe_content)

        # Execute recipe
        run_recipe_from_yaml(str(recipe_path), state_dir=str(temp_dir / ".state"))

        # Verify data in DuckDB
        conn = duckdb.connect(str(temp_dir / "output.duckdb"))
        result = conn.execute("SELECT * FROM test_table ORDER BY id").fetchall()
        columns = [desc[0] for desc in conn.execute("PRAGMA table_info(test_table)").fetchall()]
        conn.close()

        assert "first_name" in columns
        assert "last_name" in columns
        assert "fname" not in columns
        assert "lname" not in columns
        assert len(result) == 2

    def test_recipe_inheritance(self, temp_dir):
        """Test recipe execution with inheritance."""
        # Create base recipe
        base_recipe_path = temp_dir / "base.yaml"
        base_recipe_path.write_text("""
name: base_recipe

runtime:
  batch_size: 5000

transform:
  steps:
    - type: add_column
      name: _source
      value: "base"
""")

        # Create child recipe
        csv_path = temp_dir / "test_data.csv"
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "name"])
            writer.writerow([1, "Alice"])

        child_recipe_path = temp_dir / "child.yaml"
        child_recipe_content = f"""
name: child_recipe

extends: base.yaml

source:
  type: csv
  path: "{csv_path}"

transform:
  steps:
    - type: add_column
      name: _source
      value: "child"

destination:
  type: duckdb
  database: "{temp_dir / 'output.duckdb'}"
  table: test_table
  write_mode: overwrite
"""
        child_recipe_path.write_text(child_recipe_content)

        # Execute recipe
        run_recipe_from_yaml(str(child_recipe_path), state_dir=str(temp_dir / ".state"))

        # Verify inheritance: batch_size from base, transform from child
        recipe = from_yaml(str(child_recipe_path))
        assert recipe.runtime.batch_size == 5000  # From base

        # Verify data
        conn = duckdb.connect(str(temp_dir / "output.duckdb"))
        result = conn.execute("SELECT * FROM test_table").fetchall()
        conn.close()

        assert len(result) == 1
        # Should have _source column with value "child" (child overrides base)
        assert result[0][2] == "child"

    def test_incremental_load_cursor_based(self, temp_dir):
        """Test incremental loads with cursor-based strategy."""
        # Create initial CSV with timestamps
        csv_path = temp_dir / "test_data.csv"
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "name", "updated_at"])
            writer.writerow([1, "Alice", "2024-01-01 10:00:00"])
            writer.writerow([2, "Bob", "2024-01-01 11:00:00"])

        # Create recipe with incremental config
        recipe_path = temp_dir / "recipe.yaml"
        recipe_content = f"""
name: incremental_test

source:
  type: csv
  path: "{csv_path}"
  incremental:
    strategy: cursor
    cursor_column: updated_at

destination:
  type: duckdb
  database: "{temp_dir / 'output.duckdb'}"
  table: test_table
  write_mode: append
"""
        recipe_path.write_text(recipe_content)

        # First run
        state_backend = LocalStateBackend(temp_dir / ".state")
        run_recipe_from_yaml(str(recipe_path), state_dir=str(temp_dir / ".state"))

        # Verify initial load
        conn = duckdb.connect(str(temp_dir / "output.duckdb"))
        result = conn.execute("SELECT COUNT(*) FROM test_table").fetchone()
        assert result[0] == 2
        conn.close()

        # Add new rows (simulating new data)
        with open(csv_path, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([3, "Charlie", "2024-01-01 12:00:00"])
            writer.writerow([4, "David", "2024-01-01 13:00:00"])

        # Second run (should only load new rows if cursor is working)
        # Note: CSV source doesn't fully support incremental yet, but we test the structure
        run_recipe_from_yaml(str(recipe_path), state_dir=str(temp_dir / ".state"))

        # Verify state was saved
        state_file = temp_dir / ".state" / "incremental_test.json"
        assert state_file.exists()

    def test_example_recipe_execution(self, temp_dir):
        """Test executing one of the example recipes."""
        # Create a simple CSV example
        csv_path = temp_dir / "example_data.csv"
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "name"])
            writer.writerow([1, "Test"])

        # Create recipe similar to simple_csv.yaml
        recipe_path = temp_dir / "example.yaml"
        recipe_content = f"""
name: example_recipe

source:
  type: csv
  path: "{csv_path}"

transform:
  steps: []

destination:
  type: duckdb
  database: "{temp_dir / 'example.duckdb'}"
  table: example_table
  write_mode: overwrite

runtime:
  batch_size: 1000
"""
        recipe_path.write_text(recipe_content)

        # Execute using public API
        recipe = from_yaml(str(recipe_path))
        state_backend = LocalStateBackend(temp_dir / ".state")
        run_recipe(recipe, state_backend)

        # Verify execution
        conn = duckdb.connect(str(temp_dir / "example.duckdb"))
        result = conn.execute("SELECT * FROM example_table").fetchall()
        conn.close()

        assert len(result) == 1
        assert result[0] == (1, "Test")

    def test_api_from_yaml(self, temp_dir):
        """Test from_yaml API function."""
        recipe_path = temp_dir / "test.yaml"
        recipe_path.write_text("""
name: api_test

source:
  type: csv
  path: "/tmp/test.csv"

destination:
  type: duckdb
  database: "/tmp/test.duckdb"
  table: test
""")

        recipe = from_yaml(str(recipe_path))
        assert recipe.name == "api_test"
        assert recipe.source.type == "csv"
        assert recipe.destination.type == "duckdb"

    def test_api_run_recipe(self, temp_dir):
        """Test run_recipe API function."""
        # Create test CSV
        csv_path = temp_dir / "test.csv"
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "name"])
            writer.writerow([1, "Test"])

        # Create recipe
        recipe_path = temp_dir / "recipe.yaml"
        recipe_content = f"""
name: api_run_test

source:
  type: csv
  path: "{csv_path}"

destination:
  type: duckdb
  database: "{temp_dir / 'test.duckdb'}"
  table: test_table
  write_mode: overwrite
"""
        recipe_path.write_text(recipe_content)

        # Load and run
        recipe = from_yaml(str(recipe_path))
        state_backend = LocalStateBackend(temp_dir / ".state")
        run_recipe(recipe, state_backend)

        # Verify
        conn = duckdb.connect(str(temp_dir / "test.duckdb"))
        result = conn.execute("SELECT * FROM test_table").fetchall()
        conn.close()

        assert len(result) == 1

    def test_state_persistence(self, temp_dir):
        """Test that state is persisted between runs."""
        csv_path = temp_dir / "test.csv"
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "name"])
            writer.writerow([1, "Test"])

        recipe_path = temp_dir / "recipe.yaml"
        recipe_content = f"""
name: state_test

source:
  type: csv
  path: "{csv_path}"

destination:
  type: duckdb
  database: "{temp_dir / 'test.duckdb'}"
  table: test_table
  write_mode: append
"""
        recipe_path.write_text(recipe_content)

        state_dir = temp_dir / ".state"
        state_backend = LocalStateBackend(state_dir)

        # First run
        recipe = from_yaml(str(recipe_path))
        run_recipe(recipe, state_backend)

        # Verify state file exists
        state_file = state_dir / "state_test.json"
        assert state_file.exists()

        # Load state and verify it's valid
        state_dict = state_backend.load("state_test")
        assert isinstance(state_dict, dict)

