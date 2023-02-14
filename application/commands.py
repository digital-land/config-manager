# import json
# from datetime import datetime

import click

# import requests
from flask.cli import AppGroup

# from sqlalchemy import and_, select

management_cli = AppGroup("manage")


reference_tables = ["attribution", "licence", "collection", "organisation"]

test_ordered_tables = ["organisation", "typology", "collection", "dataset"]


@management_cli.command("load-data")
@click.option("--test", default=False, help="Use test ordered tables or ordered tables")
def load_data(test):

    # from application.extensions import db

    for table in reference_tables:
        pass
