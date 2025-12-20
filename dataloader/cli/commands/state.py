"""CLI command for showing recipe state."""

import json
import sys

import click

from dataloader.core.exceptions import StateError
from dataloader.core.state_backend import LocalStateBackend, create_state_backend


@click.command("show-state")
@click.argument("recipe_name")
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
    "--json",
    "json_output",
    is_flag=True,
    help="Output as JSON",
)
def show_state(recipe_name: str, state_dir: str, state_backend: str | None, json_output: bool):
    """Show current state for a recipe.
    
    Displays cursor values, watermarks, checkpoints, and metadata.
    
    Examples:
    
        dataloader show-state my_recipe
        dataloader show-state my_recipe --state-backend s3://bucket/state
        dataloader show-state my_recipe --json
    """
    try:
        # Create state backend
        if state_backend:
            backend = create_state_backend(state_backend)
        else:
            backend = LocalStateBackend(state_dir)
        
        # Load state
        state_dict = backend.load(recipe_name)
        
        if not state_dict:
            click.echo(f"No state found for recipe '{recipe_name}'")
            sys.exit(0)
        
        if json_output:
            click.echo(json.dumps(state_dict, indent=2))
        else:
            click.echo(f"State for recipe: {recipe_name}")
            click.echo("")
            
            if state_dict.get("cursor_values"):
                click.echo("Cursor Values:")
                for key, value in state_dict["cursor_values"].items():
                    click.echo(f"  {key}: {value}")
                click.echo("")
            
            if state_dict.get("watermarks"):
                click.echo("Watermarks:")
                for key, value in state_dict["watermarks"].items():
                    click.echo(f"  {key}: {value}")
                click.echo("")
            
            if state_dict.get("checkpoints"):
                click.echo(f"Checkpoints: {len(state_dict['checkpoints'])}")
                click.echo("")
            
            if state_dict.get("metadata"):
                click.echo("Metadata:")
                for key, value in state_dict["metadata"].items():
                    if key == "last_metrics":
                        click.echo(f"  {key}:")
                        for mkey, mvalue in value.items():
                            click.echo(f"    {mkey}: {mvalue}")
                    else:
                        click.echo(f"  {key}: {value}")
        
    except StateError as e:
        click.echo(f"Error loading state: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Unexpected error: {e}", err=True)
        sys.exit(1)

