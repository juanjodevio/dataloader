"""CLI command for listing available connectors."""

import click

from dataloader.connectors import list_connector_types


@click.command("list-connectors")
def list_connectors():
    """List available connectors.

    Shows all registered connector types. Connectors can support
    reading, writing, or both operations.
    """
    connector_types = list_connector_types()

    click.echo("Available Connectors:")
    for connector_type in sorted(connector_types):
        click.echo(f"  - {connector_type}")
