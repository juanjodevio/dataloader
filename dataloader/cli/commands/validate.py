"""CLI command for validating recipes."""

import sys

import click

from dataloader import from_yaml
from dataloader.core.exceptions import RecipeError


@click.command()
@click.argument("recipe_path", type=click.Path(exists=True))
@click.option(
    "--vars",
    multiple=True,
    help="CLI variables in key=value format (can be used multiple times)",
)
def validate(recipe_path: str, vars: tuple):
    """Validate a recipe YAML file.

    Checks:
    - YAML syntax
    - Recipe schema validation
    - Template variable resolution
    - Connector configuration

    Examples:

        dataloader validate recipe.yaml
        dataloader validate recipe.yaml --vars table=customers
    """
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

        # Load recipe (this validates it)
        from dataloader.models.loader import load_recipe

        recipe = load_recipe(recipe_path, cli_vars=cli_vars if cli_vars else None)

        click.echo(f"✓ Recipe '{recipe.name}' is valid")
        click.echo(f"  Source: {recipe.source.type}")
        click.echo(f"  Destination: {recipe.destination.type}")
        click.echo(f"  Transform steps: {len(recipe.transform.steps)}")
        click.echo(f"  Batch size: {recipe.runtime.batch_size}")
        click.echo(f"  Parallelism: {recipe.runtime.parallelism}")

    except RecipeError as e:
        click.echo(f"✗ Recipe validation failed: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"✗ Unexpected error: {e}", err=True)
        sys.exit(1)
