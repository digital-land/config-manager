import string
from collections import OrderedDict

from flask import Blueprint, render_template

from application.models import Dataset

schema_bp = Blueprint("schema", __name__, url_prefix="/schema")


@schema_bp.get("/")
def index():
    datasets = (
        Dataset.query.filter(
            Dataset.typology_id.not_in(["specification", "value", "entity", "category"])
        )
        .order_by(Dataset.name)
        .all()
    )
    grouped_datasets = OrderedDict()

    for letter in string.ascii_uppercase:
        group = [d for d in datasets if d.dataset[0].upper() == letter]
        if len(group) > 0:
            grouped_datasets[letter] = group

    return render_template(
        "schema/index.html", schemas=datasets, grouped_datasets=grouped_datasets
    )


@schema_bp.get("/<string:dataset_id>")
def schema(dataset_id):
    schema = Dataset.query.get(dataset_id)
    return render_template("schema/schema.html", schema=schema)
