import click
from flask.cli import AppGroup

publish_cli = AppGroup("publish")


@publish_cli.command("push")
@click.option(
    "--test", default=False, help="Show what files will be updated without committing"
)
def publish_config(test):
    pass
