from datetime import datetime

import requests
from flask.cli import AppGroup

from application.models import Collection, Endpoint, Pipeline, Source

management_cli = AppGroup("manage")

DIGITAL_LAND_DATASETTE = "https://datasette.planning.data.gov.uk/digital-land"
DIGITAL_LAND_RAW_GITHUB_URL = "https://raw.githubusercontent.com/digital-land"

reference_tables = [
    "attribution",
    "licence",
    "collection",
    "typology",
    "organisation",
    "dataset",
]


@management_cli.command("load-data")
def load_data():

    from application.extensions import db

    _load_pipeline(db)

    for table in reference_tables:
        _load_data(db, table)

    _load_config(db)


@management_cli.command("drop-data")
def drop_data():

    from application.extensions import db

    for table in reversed(db.metadata.sorted_tables):
        delete = table.delete()
        try:
            db.engine.execute(delete)
        except Exception as e:
            print(e)


def _load_data(db, table):
    url = f"{DIGITAL_LAND_DATASETTE}/{table}.json?_shape=array"
    print(f"loading from {url}")

    records = []
    while url:
        resp = requests.get(url)
        try:
            url = resp.links.get("next").get("url")
        except AttributeError:
            url = None
        records.extend(resp.json())

    for record in records:
        if table in ["attribution", "licence"]:
            del record["entity"]

        for key, val in record.items():
            if not val:
                record[key] = None
                continue
            if "date" in key:
                if not val:
                    record[key] = None
                else:
                    try:
                        record[key] = datetime.strptime(val, "%Y-%m-%d").date()
                    except Exception as e:
                        print(e)
                        record[key] = None
        try:
            t = db.metadata.tables.get(table)
            insert = t.insert().values(**record)
            db.engine.execute(insert)
        except Exception as e:
            print(e)
            print("error loading", table, "with data", record)

    print("inserted", len(records), table)


# TODO - this is wrong. We're using collection csv for a list of pipelines because there
# isn't one! Fixup where this list comes from.
def _load_pipeline(db):

    import csv

    # TODO - FIX THIS!
    url = (
        f"{DIGITAL_LAND_RAW_GITHUB_URL}/specification/main/specification/collection.csv"
    )

    resp = requests.get(url)
    content = resp.iter_lines(decode_unicode=True)

    reader = csv.DictReader(content, delimiter=",")
    rows = []
    for row in reader:
        rows.append(row)

    for row in rows:
        collection = row.pop("collection")
        row["pipeline"] = collection

        row.pop("entry-date")
        row.pop("start-date")
        row.pop("end-date")

        for key, val in row.items():
            if not val:
                row[key] = None

    for row in rows:
        pipeline = Pipeline(**row)
        db.session.add(pipeline)

    db.session.commit()


def _load_config(db):

    import csv

    file_url = "{url}/config/main/collection/{collection}/{file}.csv"

    for collection in Collection.query.all():

        url = file_url.format(
            url=DIGITAL_LAND_RAW_GITHUB_URL,
            collection=collection.collection,
            file="endpoint",
        )
        resp = requests.get(url)
        content = resp.iter_lines(decode_unicode=True)

        reader = csv.DictReader(content, delimiter=",")
        endpoints = _get_dated_rows(reader)
        endpoints = _remove_empties(endpoints)

        for endpoint in endpoints:
            # skip records with no key defined
            if not endpoint.get("endpoint"):
                print("endpoint hash missing", endpoint)
                print("===" * 30)
                continue
            data = endpoint.copy()
            data["endpoint_url"] = data.pop("endpoint-url")
            e = Endpoint(**data)
            try:
                db.session.add(e)
                db.session.commit()
            except Exception as e:
                db.session.rollback()
                print("Couldn't insert", data)
                print(e)
                print("===" * 30)

        url = file_url.format(
            url=DIGITAL_LAND_RAW_GITHUB_URL,
            collection=collection.collection,
            file="source",
        )
        resp = requests.get(url)
        content = resp.iter_lines(decode_unicode=True)

        reader = csv.DictReader(content, delimiter=",")
        sources = _get_dated_rows(reader)
        sources = _remove_empties(sources)

        for source in sources:
            # skip records with no key defined
            if not source.get("source"):
                print("source hash missing", source)
                print("===" * 30)
                continue
            data = source.copy()
            data["documentation_url"] = data.pop("documentation-url")
            pipelines = []
            if "pipelines" in data and data.get("pipelines") is not None:
                pipelines = data["pipelines"].split(";")
                data.pop("pipelines")

            s = Source(**data)
            for pipeline in pipelines:
                p = Pipeline.query.get(pipeline)
                if p:
                    s.pipelines.append(p)
            try:
                db.session.add(s)
                db.session.commit()
            except Exception as e:
                db.session.rollback()
                print("Couldn't insert", data)
                print(e)
                print("===" * 30)


def _get_dated_rows(reader):
    rows = []
    for row in reader:
        for date_field in ["entry-date", "start-date", "end-date"]:
            d = row.pop(date_field)
            update_field_name = date_field.replace("-", "_")
            if d:
                try:
                    date = datetime.strptime(d, "%Y-%m-%dT%H:%M:%SZ").date()
                    row[update_field_name] = date
                except Exception as e:
                    print(e)
                    row[update_field_name] = None
                try:
                    date = datetime.strptime(d, "%Y-%m-%d").date()
                    row[update_field_name] = date
                except Exception as e:
                    print(e)
                    row[update_field_name] = None
            else:
                row[update_field_name] = None
        rows.append(row)
    return rows


def _remove_empties(rows):
    for row in rows:
        for key, val in row.items():
            if not val:
                row[key] = None
    return rows
