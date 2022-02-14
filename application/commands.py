import requests

from flask.cli import AppGroup

from application.models import Organisation, Source, Endpoint

management_cli = AppGroup("manage")

digital_land_datasette = "https://datasette.digital-land.info/digital-land"
tables = ["organisation"]

model_classes = {"organisation": Organisation, "source": Source, "endpoint": Endpoint}


@management_cli.command("load-data")
def load_data():

    for table in tables:

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


@management_cli.command("drop-data")
def drop_data():
    from application.extensions import db
    from application.models import Organisation

    db.session.query(Organisation).delete()
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

    for i in inserts:
        obj = model_class(**i)
        db.session.add(obj)
        try:
            db.session.commit()
        except Exception as e:
            print(f"error loading {obj}")
            print(e)
