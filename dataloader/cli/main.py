"""Main CLI entry point for dataloader."""

import sys

import click

from dataloader.cli.commands.cancel import cancel
from dataloader.cli.commands.dry_run import dry_run
from dataloader.cli.commands.init import init
from dataloader.cli.commands.list import list_connectors
from dataloader.cli.commands.resume import resume
from dataloader.cli.commands.run import run
from dataloader.cli.commands.state import show_state
from dataloader.cli.commands.test_connection import test_connection
from dataloader.cli.commands.validate import validate


@click.group()
@click.version_option(version="0.2.0")
def main():
    """DataLoader - Recipe-driven data loading framework."""
    pass


# Register commands
main.add_command(run)
main.add_command(validate)
main.add_command(show_state)
main.add_command(init)
main.add_command(list_connectors)
main.add_command(test_connection)
main.add_command(dry_run)
main.add_command(resume)
main.add_command(cancel)


if __name__ == "__main__":
    main()

