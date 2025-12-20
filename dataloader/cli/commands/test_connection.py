"""CLI command for testing connections."""

import sys

import click

from dataloader import from_yaml
from dataloader.core.exceptions import ConnectorError, RecipeError


@click.command("test-connection")
@click.argument("recipe_path", type=click.Path(exists=True))
@click.option(
    "--vars",
    multiple=True,
    help="CLI variables in key=value format (can be used multiple times)",
)
def test_connection(recipe_path: str, vars: tuple):
    """Test connections for source and destination in a recipe.
    
    Attempts to connect to both source and destination to verify
    credentials and connectivity.
    
    Examples:
    
        dataloader test-connection recipe.yaml
        dataloader test-connection recipe.yaml --vars env=prod
    """
    try:
        # Parse CLI variables
        cli_vars = {}
        for var in vars:
            if "=" not in var:
                click.echo(f"Error: Invalid variable format: {var}. Use key=value", err=True)
                sys.exit(1)
            key, value = var.split("=", 1)
            cli_vars[key] = value
        
        # Load recipe
        from dataloader.models.loader import load_recipe
        recipe = load_recipe(recipe_path, cli_vars=cli_vars if cli_vars else None)
        
        click.echo(f"Testing connections for recipe: {recipe.name}")
        click.echo("")
        
        # Test source connection
        click.echo(f"Testing source connection ({recipe.source.type})...")
        try:
            from dataloader.connectors import get_source
            source = get_source(recipe.source.type, recipe.source, {})
            # Try to read a small batch to verify connection
            from dataloader.core.state import State
            state = State()
            batches = list(source.read_batches(state))
            click.echo(f"✓ Source connection successful (found {len(batches)} batches)")
        except Exception as e:
            click.echo(f"✗ Source connection failed: {e}", err=True)
            sys.exit(1)
        
        # Test destination connection
        click.echo(f"Testing destination connection ({recipe.destination.type})...")
        try:
            from dataloader.connectors import get_destination
            destination = get_destination(recipe.destination.type, recipe.destination, {})
            # For most destinations, creation is enough to verify connection
            click.echo("✓ Destination connection successful")
        except Exception as e:
            click.echo(f"✗ Destination connection failed: {e}", err=True)
            sys.exit(1)
        
        click.echo("")
        click.echo("All connections successful!")
        
    except RecipeError as e:
        click.echo(f"Recipe error: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Unexpected error: {e}", err=True)
        sys.exit(1)

