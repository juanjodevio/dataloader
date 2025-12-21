"""CLI command for resuming a failed recipe execution."""

import sys

import click

from dataloader import from_yaml, run_recipe
from dataloader.core.exceptions import (
    ConnectorError,
    EngineError,
    RecipeError,
    StateError,
    TransformError,
)
from dataloader.core.logging import configure_logging
from dataloader.core.state_backend import LocalStateBackend, create_state_backend


@click.command()
@click.argument("recipe_path", type=click.Path(exists=True))
@click.option(
    "--state-dir",
    default=".state",
    help="Directory for local state files (default: .state)",
)
@click.option(
    "--state-backend",
    help="State backend config (e.g., 's3://bucket/prefix', 'dynamodb:table')",
)
@click.option(
    "--vars",
    multiple=True,
    help="CLI variables in key=value format (can be used multiple times)",
)
@click.option(
    "--log-level",
    default="INFO",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]),
    help="Log level (default: INFO)",
)
@click.option(
    "--json-logs",
    is_flag=True,
    help="Use JSON format for logs",
)
def resume(
    recipe_path: str,
    state_dir: str,
    state_backend: str | None,
    vars: tuple,
    log_level: str,
    json_logs: bool,
):
    """Resume a recipe execution from the last saved state.

    Loads the state from the previous run and continues from where it left off.
    This is useful for recovering from failures or continuing long-running jobs.

    Examples:

        dataloader resume recipe.yaml
        dataloader resume recipe.yaml --state-backend s3://bucket/state
    """
    # Configure logging
    configure_logging(level=log_level, json_format=json_logs)

    try:
        # Parse CLI variables
        cli_vars = {}
        for var in vars:
            if "=" not in var:
                click.echo(
                    f"Error: Invalid variable format: {var}. Use key=value", err=True
                )
                sys.exit(1)
            key, value = var.split("=", 1)
            cli_vars[key] = value

        # Load recipe
        from dataloader.models.loader import load_recipe

        recipe = load_recipe(recipe_path, cli_vars=cli_vars if cli_vars else None)

        # Create state backend
        if state_backend:
            backend = create_state_backend(state_backend)
        else:
            backend = LocalStateBackend(state_dir)

        # Check if state exists
        state_dict = backend.load(recipe.name)
        if not state_dict:
            click.echo(f"No existing state found for recipe '{recipe.name}'", err=True)
            click.echo("Use 'dataloader run' to start a new execution", err=True)
            sys.exit(1)

        click.echo(f"Resuming recipe: {recipe.name}")
        click.echo("Found existing state, continuing from last checkpoint...")
        click.echo("")

        # Run recipe (will resume from state)
        run_recipe(recipe, backend)
        click.echo("Recipe execution completed successfully")

    except RecipeError as e:
        click.echo(f"Recipe error: {e}", err=True)
        sys.exit(1)
    except (EngineError, ConnectorError, TransformError, StateError) as e:
        click.echo(f"Execution error: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Unexpected error: {e}", err=True)
        sys.exit(1)
