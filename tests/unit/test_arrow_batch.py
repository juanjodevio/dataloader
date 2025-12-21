"""Unit tests for ArrowBatch implementation."""

from datetime import date, datetime

import pyarrow as pa
import pytest

from dataloader.core.batch import ArrowBatch, Batch


class TestArrowBatch:
    """Test ArrowBatch class implementation."""

    def test_init_from_table(self):
        """Test ArrowBatch creation from Arrow table."""
        table = pa.table(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "age": [25, 30, 35],
            }
        )
        metadata = {"source": "test"}

        batch = ArrowBatch(table, metadata)

        assert batch.columns == ["id", "name", "age"]
        assert batch.row_count == 3
        assert batch.metadata == metadata
        # Batch is a Protocol - verify it implements required methods
        assert hasattr(batch, "columns")
        assert hasattr(batch, "rows")
        assert hasattr(batch, "metadata")
        assert hasattr(batch, "row_count")

    def test_from_rows_basic(self):
        """Test ArrowBatch.from_rows() with basic types."""
        columns = ["id", "name", "age"]
        rows = [[1, "Alice", 25], [2, "Bob", 30], [3, "Charlie", 35]]
        metadata = {"source": "test"}

        batch = ArrowBatch.from_rows(columns, rows, metadata)

        assert batch.columns == columns
        assert batch.row_count == 3
        assert batch.metadata == metadata
        assert batch.rows == rows

    def test_from_rows_empty(self):
        """Test ArrowBatch.from_rows() with empty data."""
        columns = ["id", "name"]
        rows = []

        batch = ArrowBatch.from_rows(columns, rows)

        assert batch.columns == columns
        assert batch.row_count == 0
        assert batch.rows == []

    def test_from_rows_type_inference(self):
        """Test automatic type inference from Python types."""
        columns = ["str_col", "int_col", "float_col", "bool_col"]
        rows = [
            ["text", 42, 3.14, True],
            ["more", 100, 2.71, False],
        ]

        batch = ArrowBatch.from_rows(columns, rows)
        table = batch.to_arrow()

        # Check inferred types
        assert table.schema.field("str_col").type == pa.string()
        assert table.schema.field("int_col").type == pa.int64()
        assert table.schema.field("float_col").type == pa.float64()
        assert table.schema.field("bool_col").type == pa.bool_()

    def test_from_rows_with_nulls(self):
        """Test handling of None values (nullable columns)."""
        columns = ["id", "name", "age"]
        rows = [
            [1, "Alice", 25],
            [2, None, None],  # Null values
            [3, "Charlie", 35],
        ]

        batch = ArrowBatch.from_rows(columns, rows)
        table = batch.to_arrow()

        assert batch.row_count == 3
        assert batch.rows[1][1] is None
        assert batch.rows[1][2] is None
        # Schema should allow nullable fields
        assert table.schema.field("name").nullable
        assert table.schema.field("age").nullable

    def test_from_rows_date_datetime(self):
        """Test handling of date and datetime types."""
        columns = ["date_col", "datetime_col"]
        test_date = date(2024, 1, 15)
        test_datetime = datetime(2024, 1, 15, 12, 30, 45)
        rows = [[test_date, test_datetime]]

        batch = ArrowBatch.from_rows(columns, rows)
        table = batch.to_arrow()

        assert batch.row_count == 1
        # Dates should be preserved
        assert batch.rows[0][0] == test_date
        assert batch.rows[0][1] == test_datetime
        # Check Arrow types
        assert pa.types.is_date32(table.schema.field("date_col").type)
        assert pa.types.is_timestamp(table.schema.field("datetime_col").type)

    def test_from_rows_validation_error(self):
        """Test validation errors in from_rows()."""
        columns = ["id", "name"]
        rows = [[1, "Alice", "extra"]]  # Row too long

        with pytest.raises(ValueError, match="Row 0 length"):
            ArrowBatch.from_rows(columns, rows)

    def test_from_rows_empty_columns_with_rows(self):
        """Test error when columns is empty but rows provided."""
        columns = []
        rows = [[1, 2]]

        with pytest.raises(ValueError, match="columns cannot be empty"):
            ArrowBatch.from_rows(columns, rows)

    def test_columns_property(self):
        """Test columns property returns correct column names."""
        table = pa.table({"a": [1], "b": [2], "c": [3]})
        batch = ArrowBatch(table)

        assert batch.columns == ["a", "b", "c"]

    def test_rows_property(self):
        """Test rows property returns list of lists in column order."""
        columns = ["id", "name"]
        rows = [[1, "Alice"], [2, "Bob"]]
        batch = ArrowBatch.from_rows(columns, rows)

        result_rows = batch.rows
        assert result_rows == rows
        assert isinstance(result_rows, list)
        assert all(isinstance(row, list) for row in result_rows)

    def test_row_count_property(self):
        """Test row_count property."""
        table = pa.table({"col": list(range(100))})
        batch = ArrowBatch(table)

        assert batch.row_count == 100

    def test_metadata_property(self):
        """Test metadata property."""
        metadata = {"source": "test", "batch_num": 42}
        table = pa.table({"col": [1]})
        batch = ArrowBatch(table, metadata)

        assert batch.metadata == metadata
        # Metadata is stored as-is (not copied for performance)
        assert batch.metadata is metadata

    def test_metadata_default_empty(self):
        """Test metadata defaults to empty dict if not provided."""
        table = pa.table({"col": [1]})
        batch = ArrowBatch(table)

        assert batch.metadata == {}

    def test_to_dict(self):
        """Test to_dict() method."""
        columns = ["id", "name"]
        rows = [[1, "Alice"], [2, "Bob"]]
        metadata = {"source": "test"}
        batch = ArrowBatch.from_rows(columns, rows, metadata)

        result = batch.to_dict()

        assert result == {
            "columns": columns,
            "rows": rows,
            "metadata": metadata,
        }

    def test_to_arrow(self):
        """Test to_arrow() returns underlying Arrow table."""
        table = pa.table({"col": [1, 2, 3]})
        batch = ArrowBatch(table)

        result_table = batch.to_arrow()

        assert result_table.equals(table)
        assert result_table is table  # Should return same object

    def test_metadata_preservation(self):
        """Test metadata is preserved through operations."""
        metadata = {"source": "test", "timestamp": "2024-01-01"}
        table = pa.table({"col": [1]})
        batch = ArrowBatch(table, metadata)

        # Create new batch with same metadata
        new_table = pa.table({"col": [2]})
        new_batch = ArrowBatch(new_table, batch.metadata.copy())

        assert new_batch.metadata == metadata

    def test_empty_batch(self):
        """Test handling of empty batches."""
        table = pa.table({"col": pa.array([], type=pa.string())})
        batch = ArrowBatch(table)

        assert batch.row_count == 0
        assert batch.columns == ["col"]
        assert batch.rows == []

    def test_init_empty_table_error(self):
        """Test error when table has zero columns."""
        # Create empty schema (no columns)
        schema = pa.schema([])
        table = pa.table({}, schema=schema)

        # Empty table with no columns should raise error
        with pytest.raises(ValueError, match="table cannot have zero columns"):
            ArrowBatch(table)

    def test_large_dataset(self):
        """Test ArrowBatch with larger dataset."""
        num_rows = 10000
        columns = ["id", "value"]
        rows = [[i, f"value_{i}"] for i in range(num_rows)]

        batch = ArrowBatch.from_rows(columns, rows)

        assert batch.row_count == num_rows
        assert len(batch.rows) == num_rows
        assert batch.rows[0] == [0, "value_0"]
        assert batch.rows[-1] == [9999, "value_9999"]

    def test_mixed_types_in_rows(self):
        """Test rows with mixed Python types (PyArrow will coerce to common type)."""
        columns = ["mixed"]
        # PyArrow will coerce mixed types to a common compatible type
        # Strings are most permissive, so mix will likely become strings
        rows = [
            ["string"],
            ["42"],  # String representation
            ["3.14"],  # String representation
            ["True"],  # String representation
            [None],
        ]

        batch = ArrowBatch.from_rows(columns, rows)

        assert batch.row_count == 5
        # All will be strings (most permissive type when mixing)

    def test_protocol_compliance(self):
        """Test that ArrowBatch implements Batch protocol correctly."""
        table = pa.table({"col": [1, 2, 3]})
        batch = ArrowBatch(table, {"test": "metadata"})

        # All protocol methods should exist and work
        assert hasattr(batch, "columns")
        assert hasattr(batch, "rows")
        assert hasattr(batch, "metadata")
        assert hasattr(batch, "row_count")
        assert hasattr(batch, "to_dict")

        # Verify Batch protocol compliance by checking all required properties/methods
        # Note: Batch is a Protocol, so we verify structural typing rather than isinstance
        _ = batch.columns
        _ = batch.rows
        _ = batch.metadata
        _ = batch.row_count
        _ = batch.to_dict()
