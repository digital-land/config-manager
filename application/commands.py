import json

import requests

from flask.cli import AppGroup

from application.models import (
    Organisation,
    Source,
    Endpoint,
    Typology,
    Collection,
    Dataset,
)

management_cli = AppGroup("manage")

digital_land_datasette = "https://datasette.digital-land.info/digital-land"

model_classes = {
    "organisation": Organisation,
    "source": Source,
    "endpoint": Endpoint,
    "typology": Typology,
    "collection": Collection,
    "dataset": Dataset,
}

ordered_tables = [
    "organisation",
    "typology",
    "collection",
    "dataset",
    "endpoint",
    "source",
]


@management_cli.command("load-data")
def load_data():

    for table in ordered_tables:
        url = f"{digital_land_datasette}/{table}.json"
        print(f"loading from {url}")

        has_next = True
        while has_next:
            resp = requests.get(url)
            resp.raise_for_status()
            data = resp.json()

            if data.get("next") is None:
                has_next = False

            columns = data["columns"]
            table = data["table"]
            rows = data["rows"]

            _load_data(columns, table, rows)
            url = data.get("next_url")
            if url is None:
                has_next = False


@management_cli.command("drop-data")
def drop_data():
    from application.extensions import db

    for table in reversed(ordered_tables):

        model_class = model_classes.get(table)
        if model_class is not None:
            db.session.query(model_class).delete()
            db.session.commit()


def _load_data(columns, table, rows):

    from application.extensions import db

    model_class = model_classes.get(table)

    if model_class is None:
        raise Exception(f"Can't load data for {table}")

    inserts = []
    for row in rows:
        r = [None if not item else item for item in row]
        inserts.append(dict(zip(columns, r)))

    if table == "dataset":
        for insert in inserts:
            if "paint_options" in insert:
                insert["paint_options"] = (
                    json.loads(insert["paint_options"])
                    if insert["paint_options"]
                    else None
                )

    for i in inserts:
        obj = model_class(**i)
        if db.session.query(model_class).get(i[table]) is None:
            db.session.add(obj)
            try:
                db.session.commit()
            except Exception as e:
                print(f"error loading {obj}")
                print(e)
        else:
            print(f"Row for {table}: {i[table]} already in db")
