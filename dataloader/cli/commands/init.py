"""CLI command for initializing a new recipe project."""

import sys
from pathlib import Path

import click


@click.command()
@click.option(
    "--recipe-name",
    default="my_recipe",
    help="Name for the example recipe file (default: my_recipe)",
)
@click.option(
    "--output-dir",
    default=".",
    type=click.Path(),
    help="Directory to create recipe file in (default: current directory)",
)
def init(recipe_name: str, output_dir: str):
    """Initialize a new recipe project.

    Creates:
    - Example recipe YAML file
    - .state directory
    - .gitignore entries

    Examples:

        dataloader init
        dataloader init --recipe-name customers
        dataloader init --output-dir recipes/
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    recipe_file = output_path / f"{recipe_name}.yaml"
    state_dir = output_path / ".state"
    gitignore_file = output_path / ".gitignore"

    # Create example recipe
    recipe_content = f"""name: {recipe_name}

source:
  type: csv
  path: data/input.csv

transform:
  steps:
    - type: add_column
      name: _loaded_at
      value: "{{{{ recipe.name }}}}"

destination:
  type: duckdb
  database: output.duckdb
  table: {recipe_name}
  write_mode: overwrite

runtime:
  batch_size: 10000
  parallelism: 1
"""

    if recipe_file.exists():
        click.echo(f"Recipe file already exists: {recipe_file}", err=True)
        click.echo("Use --recipe-name to specify a different name", err=True)
        sys.exit(1)

    recipe_file.write_text(recipe_content)
    click.echo(f"Created recipe file: {recipe_file}")

    # Create .state directory
    state_dir.mkdir(parents=True, exist_ok=True)
    click.echo(f"Created state directory: {state_dir}")

    # Update .gitignore
    gitignore_entries = [
        "# DataLoader state files",
        ".state/",
        "*.duckdb",
        "*.duckdb.wal",
    ]

    existing_entries = set()
    if gitignore_file.exists():
        existing_entries = set(gitignore_file.read_text().splitlines())

    new_entries = [
        entry for entry in gitignore_entries if entry not in existing_entries
    ]
    if new_entries:
        with gitignore_file.open("a") as f:
            f.write("\n" + "\n".join(new_entries) + "\n")
        click.echo(f"Updated .gitignore: {gitignore_file}")
    else:
        click.echo(f".gitignore already contains entries: {gitignore_file}")

    click.echo("")
    click.echo("Next steps:")
    click.echo(f"  1. Edit {recipe_file} with your source and destination")
    click.echo(f"  2. Run: dataloader run {recipe_file}")
