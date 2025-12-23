import pyarrow as pa

from dataloader.core.schema import (
    Column,
    ContractMode,
    Schema,
    SchemaContracts,
    SchemaEvolution,
    SchemaMode,
    SchemaValidator,
    TypeInferrer,
)


def test_infer_shallow_keeps_top_level_only():
    struct_field = pa.field(
        "user",
        pa.struct(
            [
                pa.field("id", pa.int64()),
                pa.field("name", pa.string()),
            ]
        ),
        nullable=False,
    )
    table = pa.Table.from_batches([], schema=pa.schema([struct_field]))

    inferrer = TypeInferrer(reflection_level="shallow")
    result = inferrer.infer(table)

    assert [c.name for c in result.schema.columns] == ["user"]
    assert result.schema.columns[0].type.startswith("struct")
    assert result.reflection_level == "shallow"


def test_infer_deep_flattens_struct():
    struct_field = pa.field(
        "user",
        pa.struct(
            [
                pa.field("id", pa.int64()),
                pa.field("name", pa.string()),
            ]
        ),
        nullable=False,
    )
    table = pa.Table.from_batches([], schema=pa.schema([struct_field]))

    inferrer = TypeInferrer(reflection_level="deep")
    result = inferrer.infer(table)

    col_names = [c.name for c in result.schema.columns]
    assert set(col_names) == {"user__id", "user__name"}
    assert result.reflection_level == "deep"


def test_schema_evolution_creates_variant_on_type_change():
    current = Schema(columns=[Column(name="a", type="int64")])
    incoming = Schema(columns=[Column(name="a", type="string")])

    evo = SchemaEvolution()
    updated, diff = evo.apply(current, incoming)

    assert "a" in diff.type_changes
    assert any(name.startswith("a__v_string") for name in diff.variant_columns)
    assert any(col.name.startswith("a__v_string") for col in updated.columns)
    # original column remains
    assert any(col.name == "a" and col.type == "int64" for col in updated.columns)


def test_schema_evolution_adds_new_column():
    current = Schema(columns=[])
    incoming = Schema(columns=[Column(name="b", type="string", nullable=True)])

    evo = SchemaEvolution()
    updated, diff = evo.apply(current, incoming)

    assert diff.added_columns == ["b"]
    assert any(col.name == "b" for col in updated.columns)


def test_validate_strict_extra_column_errors():
    schema = Schema(columns=[Column(name="a", type="int64")], mode=SchemaMode.STRICT)
    table = pa.table({"a": [1], "b": [2]})

    validator = SchemaValidator(mode=SchemaMode.STRICT)
    result = validator.validate(table, schema)

    assert not result.ok
    assert any("unexpected column 'b'" in issue.message for issue in result.errors)
    assert "b" not in [c.name for c in result.validated_schema.columns]


def test_validate_infer_adds_column_when_evolve():
    schema = Schema(columns=[Column(name="a", type="int64")], mode=SchemaMode.INFER)
    table = pa.table({"a": [1], "b": ["x"]})

    validator = SchemaValidator(mode=SchemaMode.INFER)
    result = validator.validate(table, schema)

    assert result.ok
    assert any(issue.level == "warning" for issue in result.warnings)
    assert {"a", "b"} == {c.name for c in result.validated_schema.columns}


def test_validate_discard_columns_contract_drops_extra():
    schema = Schema(columns=[Column(name="a", type="int64")], mode=SchemaMode.STRICT)
    table = pa.table({"a": [1], "b": [2]})
    contracts = SchemaContracts(columns={"b": ContractMode.DISCARD_COLUMNS})

    validator = SchemaValidator(mode=SchemaMode.STRICT, contracts=contracts)
    result = validator.validate(table, schema)

    assert result.ok
    assert "b" in result.dropped_columns
    assert not result.errors


def test_validate_freeze_type_mismatch_errors():
    schema = Schema(columns=[Column(name="a", type="int64")])
    table = pa.table({"a": ["x"]})
    contracts = SchemaContracts(columns={"a": ContractMode.FREEZE})

    validator = SchemaValidator(mode=SchemaMode.INFER, contracts=contracts)
    result = validator.validate(table, schema)

    assert not result.ok
    assert any("type mismatch" in issue.message for issue in result.errors)


def test_validate_discard_rows_drops_batch():
    schema = Schema(columns=[Column(name="a", type="int")])
    table = pa.table({"a": [1], "extra": [2]})
    contracts = SchemaContracts(columns={"extra": ContractMode.DISCARD_ROWS})

    validator = SchemaValidator(mode=SchemaMode.STRICT, contracts=contracts)
    result = validator.validate(table, schema)

    assert result.dropped_rows is True
    assert result.ok  # no errors, rows dropped
