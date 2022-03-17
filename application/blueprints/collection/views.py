from flask import Blueprint, abort, send_file

from application.extensions import db
from application.models import Dataset, Endpoint, Source, source_dataset
from application.utils import csv_data_to_buffer

collection_bp = Blueprint("collection", __name__, url_prefix="/collection")


@collection_bp.get("/<collection>/endpoint.csv")
def collection_endpoint_csv(collection):
    endpoints = (
        db.session.query(Endpoint)
        .filter(Endpoint.endpoint == Source.endpoint_id)
        .filter(Source.source == source_dataset.c.source)
        .filter(Dataset.dataset == source_dataset.c.dataset)
        .filter(Dataset.collection == collection)
        .order_by(Endpoint.entry_date)
        .distinct()
        .all()
    )
    if not endpoints:
        return abort(404)
    csv_rows = []
    for endpoint in endpoints:
        csv_rows.append(endpoint.to_csv_dict())
    buffer = csv_data_to_buffer(csv_rows)
    return send_file(
        buffer,
        as_attachment=True,
        attachment_filename="endpoint.csv",
        mimetype="text/csv",
    )


@collection_bp.get("/<collection>/source.csv")
def collection_source_csv(collection):
    sources = (
        db.session.query(Source)
        .filter(Source.source == source_dataset.c.source)
        .filter(Dataset.dataset == source_dataset.c.dataset)
        .filter(Dataset.collection == collection)
        .order_by(Source.entry_date)
        .distinct()
        .all()
    )
    if not sources:
        return abort(404)

    csv_rows = [s.to_csv_dict() for s in sources]
    buffer = csv_data_to_buffer(csv_rows)
    return send_file(
        buffer,
        as_attachment=True,
        attachment_filename="source.csv",
        mimetype="text/csv",
    )
