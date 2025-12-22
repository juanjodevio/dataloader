"""Recipe model combining all configuration components."""

from typing import Optional

from pydantic import BaseModel, Field, ConfigDict

from dataloader.models.destination_config import DestinationConfig
from dataloader.models.runtime_config import RuntimeConfig
from dataloader.models.schema_config import SchemaConfig
from dataloader.models.source_config import SourceConfig
from dataloader.models.transform_config import TransformConfig


class Recipe(BaseModel):
    """Complete recipe definition for data loading pipeline."""

    model_config = ConfigDict(protected_namespaces=(), populate_by_name=True)

    name: str = Field(description="Recipe name (required)")
    extends: Optional[str] = Field(
        default=None, description="Path to parent recipe file for inheritance"
    )
    source: SourceConfig = Field(description="Source configuration")
    transform: TransformConfig = Field(description="Transform configuration")
    destination: DestinationConfig = Field(description="Destination configuration")
    runtime: RuntimeConfig = Field(
        default_factory=RuntimeConfig, description="Runtime configuration"
    )
    schema_config: SchemaConfig | None = Field(
        default=None,
        description="Optional schema configuration",
        alias="schema",
    )

    @classmethod
    def from_dict(cls, data: dict) -> "Recipe":
        """Create Recipe from dictionary (after inheritance resolution)."""
        return cls(**data)

    @classmethod
    def from_yaml(cls, path: str, cli_vars: dict[str, str] | None = None) -> "Recipe":
        """
        Load recipe from YAML file with inheritance resolution.

        Args:
            path: Path to recipe YAML file
            cli_vars: Variables passed via CLI (e.g., --vars key=value)

        Returns:
            Fully resolved Recipe instance
        """
        from dataloader.models.loader import load_recipe

        return load_recipe(path, cli_vars)
