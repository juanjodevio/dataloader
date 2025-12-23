from dataloader.core.schema import (
    Column,
    Schema,
    SchemaEvolution,
    SchemaLineage,
    SchemaRegistry,
)


def test_registry_register_and_get_inmemory():
    registry = SchemaRegistry()
    schema = Schema(columns=[Column(name="id", type="int")])

    version = registry.register("recipe1", schema)
    fetched = registry.get("recipe1", version)

    assert fetched is not None
    assert fetched.columns[0].name == "id"
    assert version in registry.list_versions("recipe1")


def test_registry_compare_with_evolution():
    evolution = SchemaEvolution()
    registry = SchemaRegistry(evolution=evolution)

    v1 = Schema(columns=[Column(name="a", type="int")])
    v2 = Schema(columns=[Column(name="a", type="string"), Column(name="b", type="int")])

    v1_id = registry.register("recipe2", v1, version="v1")
    update = registry.compare("recipe2", v1_id, v2)

    assert update is not None
    assert "a" in update.type_changes
    assert "b" in update.added_columns


def test_lineage_tracks_versions():
    lineage = SchemaLineage()
    registry = SchemaRegistry(lineage=lineage)

    v1 = Schema(columns=[Column(name="a", type="int")])
    v2 = Schema(columns=[Column(name="a", type="string")])

    registry.register("recipe3", v1, version="v1")
    registry.register("recipe3", v2, version="v2")

    entries = lineage.get("recipe3", "a")
    assert len(entries) == 2
    assert {e.version for e in entries} == {"v1", "v2"}
