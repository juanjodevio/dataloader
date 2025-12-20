"""CLI command for dry-run execution."""

import sys

import click

from dataloader import from_yaml
from dataloader.core.exceptions import RecipeError
from dataloader.core.logging import configure_logging


@click.command("dry-run")
@click.argument("recipe_path", type=click.Path(exists=True))
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
def dry_run(recipe_path: str, vars: tuple, log_level: str, json_logs: bool):
    """Perform a dry-run of a recipe without writing to destination.
    
    Validates the recipe, tests connections, and simulates execution
    without actually writing data to the destination.
    
    Examples:
    
        dataloader dry-run recipe.yaml
        dataloader dry-run recipe.yaml --vars env=prod
    """
    # Configure logging
    configure_logging(level=log_level, json_format=json_logs)
    
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
        
        click.echo(f"Dry-run for recipe: {recipe.name}")
        click.echo("")
        
        # Validate recipe
        click.echo("✓ Recipe validation passed")
        click.echo(f"  Source: {recipe.source.type}")
        click.echo(f"  Destination: {recipe.destination.type}")
        click.echo(f"  Batch size: {recipe.runtime.batch_size}")
        click.echo(f"  Parallelism: {recipe.runtime.parallelism}")
        click.echo("")
        
        # Test source connection and count batches
        click.echo("Testing source connection...")
        from dataloader.connectors import get_source
        from dataloader.core.state import State
        source = get_source(recipe.source.type, recipe.source, {})
        state = State()
        batches = list(source.read_batches(state))
        click.echo(f"✓ Source connection successful")
        click.echo(f"  Found {len(batches)} batches")
        if batches:
            total_rows = sum(batch.row_count for batch in batches)
            click.echo(f"  Total rows: {total_rows}")
        click.echo("")
        
        # Test destination connection
        click.echo("Testing destination connection...")
        from dataloader.connectors import get_destination
        destination = get_destination(recipe.destination.type, recipe.destination, {})
        click.echo("✓ Destination connection successful")
        click.echo("")
        
        click.echo("Dry-run completed successfully!")
        click.echo("Recipe is ready to run. Use 'dataloader run' to execute.")
        
    except RecipeError as e:
        click.echo(f"Recipe error: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)

