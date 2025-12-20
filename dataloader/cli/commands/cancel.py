"""CLI command for canceling a running recipe execution."""

import sys

import click

from dataloader.core.exceptions import StateError
from dataloader.core.state_backend import LocalStateBackend, create_state_backend


@click.command()
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
def cancel(recipe_name: str, state_dir: str, state_backend: str | None):
    """Cancel a running recipe execution.
    
    Note: This command marks the recipe as canceled in the state.
    For actually stopping a running process, use Ctrl+C or kill the process.
    This is mainly useful for marking a recipe as canceled in shared state backends.
    
    Examples:
    
        dataloader cancel my_recipe
        dataloader cancel my_recipe --state-backend s3://bucket/state
    """
    try:
        # Create state backend
        if state_backend:
            backend = create_state_backend(state_backend)
        else:
            backend = LocalStateBackend(state_dir)
        
        # Load current state
        state_dict = backend.load(recipe_name)
        if not state_dict:
            click.echo(f"No state found for recipe '{recipe_name}'", err=True)
            sys.exit(1)
        
        # Mark as canceled in metadata
        if "metadata" not in state_dict:
            state_dict["metadata"] = {}
        state_dict["metadata"]["canceled"] = True
        state_dict["metadata"]["canceled_at"] = __import__("datetime").datetime.utcnow().isoformat()
        
        # Save updated state
        backend.save(recipe_name, state_dict)
        
        click.echo(f"Recipe '{recipe_name}' marked as canceled")
        
    except StateError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Unexpected error: {e}", err=True)
        sys.exit(1)

