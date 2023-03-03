from flask import Blueprint, render_template

from application.models import Dataset

schema_bp = Blueprint("schema", __name__, url_prefix="/schema")


@schema_bp.get("/")
def index():
    datasets = Dataset.query.order_by(Dataset.name).all()
    return render_template("schema/index.html", schemas=datasets)


@schema_bp.get("/<string:dataset_id>")
def schema(dataset_id):
    schema = Dataset.query.get(dataset_id)
    return render_template("schema/schema.html", schema=schema)
