"""CLI command for listing available connectors."""

import click

from dataloader.connectors import list_destination_types, list_source_types


@click.command("list-connectors")
def list_connectors():
    """List available source and destination connectors.
    
    Shows all registered connector types and their capabilities.
    """
    source_types = list_source_types()
    destination_types = list_destination_types()
    
    click.echo("Available Source Connectors:")
    for source_type in sorted(source_types):
        click.echo(f"  - {source_type}")
    
    click.echo("")
    click.echo("Available Destination Connectors:")
    for dest_type in sorted(destination_types):
        click.echo(f"  - {dest_type}")

