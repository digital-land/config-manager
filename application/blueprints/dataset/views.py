from flask import Blueprint, abort, send_file

from application.extensions import db
from application.models import Dataset, Endpoint, Source, source_dataset
from application.utils import csv_data_to_buffer

dataset_bp = Blueprint("dataset", __name__, url_prefix="/dataset")


@dataset_bp.get("/<dataset>/endpoint.csv")
def dataset_endpoint_csv(dataset):
    endpoints = (
        db.session.query(Endpoint)
        .filter(Endpoint.endpoint == Source.endpoint_id)
        .filter(Source.source == source_dataset.c.source)
        .filter(Dataset.dataset == source_dataset.c.dataset)
        .filter(Dataset.dataset == dataset)
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


@dataset_bp.get("/<dataset>/source.csv")
def dataset_source_csv(dataset):
    dataset = Dataset.query.get(dataset)
    if not dataset:
        return abort(404)
    csv_rows = []
    for source in dataset.sources:
        csv_rows.append(source.to_csv_dict())
    buffer = csv_data_to_buffer(csv_rows)
    return send_file(
        buffer,
        as_attachment=True,
        attachment_filename="source.csv",
        mimetype="text/csv",
    )
