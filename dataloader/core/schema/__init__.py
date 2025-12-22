"""Schema package: models, inference, and evolution helpers."""

from dataloader.core.schema.contracts import ContractMode, SchemaContracts
from dataloader.core.schema.evolution import SchemaEvolution, SchemaUpdate
from dataloader.core.schema.inference import InferenceResult, TypeInferrer
from dataloader.core.schema.lineage import ColumnLineageEntry, SchemaLineage
from dataloader.core.schema.models import Column, EvolutionPolicy, Schema, SchemaMode
from dataloader.core.schema.registry import SchemaRegistry
from dataloader.core.schema.storage import (
    InMemorySchemaStorage,
    LocalJsonSchemaStorage,
    SchemaStorage,
)
from dataloader.core.schema.validation import (
    SchemaValidator,
    ValidationIssue,
    ValidationResult,
)

__all__ = [
    "Column",
    "EvolutionPolicy",
    "Schema",
    "SchemaMode",
    "SchemaValidator",
    "SchemaContracts",
    "SchemaRegistry",
    "SchemaLineage",
    "ColumnLineageEntry",
    "SchemaStorage",
    "InMemorySchemaStorage",
    "LocalJsonSchemaStorage",
    "ContractMode",
    "ValidationIssue",
    "ValidationResult",
    "TypeInferrer",
    "InferenceResult",
    "SchemaEvolution",
    "SchemaUpdate",
]

