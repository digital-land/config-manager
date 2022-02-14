import click

from flask.cli import AppGroup

manage_cli = AppGroup("manage")


@manage_cli.command("load")
@click.option("--data")
def load_data(data):
    print(f"loading {data}")
