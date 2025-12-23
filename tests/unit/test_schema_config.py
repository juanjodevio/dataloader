from dataloader.core.schema import ContractMode, SchemaMode
from dataloader.models.schema_config import ColumnConfig, SchemaConfig


def test_schema_config_to_schema():
    cfg = SchemaConfig(
        mode=SchemaMode.STRICT,
        columns=[ColumnConfig(name="id", type="int", nullable=False)],
    )

    schema = cfg.to_schema()

    assert schema.mode == SchemaMode.STRICT
    assert len(schema.columns) == 1
    assert schema.columns[0].name == "id"
    assert schema.columns[0].nullable is False


def test_schema_config_contract_defaults():
    cfg = SchemaConfig()
    assert cfg.contracts.default == ContractMode.EVOLVE


def test_schema_config_reflection_level_field():
    cfg = SchemaConfig(reflection_level="shallow")
    assert cfg.reflection_level == "shallow"
