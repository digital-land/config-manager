import json

import click
import requests
from flask.cli import AppGroup
from sqlalchemy import and_, select

from application.models import (
    Collection,
    Column,
    Dataset,
    Datatype,
    Endpoint,
    Field,
    Organisation,
    Resource,
    Source,
    Typology,
    resource_endpoint,
    source_dataset,
)

management_cli = AppGroup("manage")

digital_land_datasette = "https://development-datasette.digital-land.info/digital-land"
model_classes = {
    "organisation": Organisation,
    "typology": Typology,
    "collection": Collection,
    "dataset": Dataset,
    "endpoint": Endpoint,
    "source": Source,
    "resource": Resource,
    "resource_endpoint": resource_endpoint,
    "source_pipeline": source_dataset,
    "datatype": Datatype,
    "field": Field,
    "column": Column,
}


ordered_tables = model_classes.keys()

foreign_key_columns = [
    "organisation",
    "endpoint",
    "collection",
    "resource",
    "datatype",
    "typology",
    "dataset",
    "field",
]

test_ordered_tables = ["organisation", "typology", "collection", "dataset"]


@management_cli.command("load-data")
@click.option("--test", default=False, help="Use test ordered tables or ordered tables")
def load_data(test):

    tables = ordered_tables if not test else test_ordered_tables
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

    if table == "resource_endpoint" or table == "source_pipeline":
        for insert in inserts:
            del insert["rowid"]

    for i in inserts:

        if table == "resource_endpoint":
            ins = model_class.insert().values(**i)
            conn = db.engine.connect()
            s = select(model_class).where(
                and_(
                    model_class.c.resource == i["resource"],
                    model_class.c.endpoint == i["endpoint"],
                )
            )
            result = conn.execute(s).fetchone()
            if not result:
                conn.execute(ins)
            else:
                print(f"{i} already in db")

        elif table == "source_pipeline":
            # note source and destination table have different names
            i["dataset"] = i.pop("pipeline")
            ins = model_class.insert().values(**i)
            conn = db.engine.connect()
            s = select(model_class).where(
                and_(
                    model_class.c.source == i["source"],
                    model_class.c.dataset == i["dataset"],
                )
            )
            result = conn.execute(s).fetchone()
            if not result:
                conn.execute(ins)
            else:
                print(f"{i} already in db")
        else:
            try:
                if table in ["source", "field", "column"]:
                    for fk_col in foreign_key_columns:
                        if table != fk_col:
                            key = i.pop(fk_col, None)
                            if key is not None:
                                one_to_many_class = model_classes[fk_col]
                                related_obj = one_to_many_class.query.get(key)
                                if related_obj is not None:
                                    i[fk_col] = related_obj

                if db.session.query(model_class).get(i[table]) is None:
                    obj = model_class(**i)
                    db.session.add(obj)
                    db.session.commit()
                else:
                    print(f"{table}: {i[table]} already loaded")

            except Exception as e:
                print(f"error loading {i}")
                print(e)
