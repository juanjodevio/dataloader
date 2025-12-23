"""Core module for dataloader package."""

from dataloader.core.batch import ArrowBatch, Batch
from dataloader.core.engine import execute
from dataloader.core.exceptions import (
    ConnectorError,
    DataLoaderError,
    EngineError,
    RecipeError,
    StateError,
    TransformError,
)
from dataloader.core.schema import (
    Column,
    ColumnLineageEntry,
    ContractMode,
    EvolutionPolicy,
    InferenceResult,
    InMemorySchemaStorage,
    LocalJsonSchemaStorage,
    Schema,
    SchemaContracts,
    SchemaEvolution,
    SchemaLineage,
    SchemaMode,
    SchemaRegistry,
    SchemaStorage,
    SchemaUpdate,
    SchemaValidator,
    TypeInferrer,
    ValidationIssue,
    ValidationResult,
)
from dataloader.core.state import State
from dataloader.core.state_backend import LocalStateBackend, StateBackend

__all__ = [
    "Batch",
    "ArrowBatch",
    "State",
    "StateBackend",
    "LocalStateBackend",
    "DataLoaderError",
    "RecipeError",
    "ConnectorError",
    "TransformError",
    "StateError",
    "EngineError",
    "execute",
    "Schema",
    "Column",
    "ContractMode",
    "SchemaMode",
    "EvolutionPolicy",
    "SchemaValidator",
    "SchemaContracts",
    "TypeInferrer",
    "InferenceResult",
    "SchemaEvolution",
    "SchemaUpdate",
    "ValidationIssue",
    "ValidationResult",
]
