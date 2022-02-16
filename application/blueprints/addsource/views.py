from flask import Blueprint, redirect, render_template, session, url_for

from application.blueprints.addsource.forms import SourceForm
from application.models import Dataset, Endpoint, Organisation
from application.utils import compute_hash

addsource = Blueprint("addsource", __name__, url_prefix="/add-a-source")


@addsource.route("/", methods=["GET", "POST"])
def index():
    form = SourceForm()
    organisations = Organisation.query.order_by(Organisation.name).all()
    form.organisation.choices = [("", "")] + [
        (o.organisation, o.name) for o in organisations
    ]
    datasets = (
        Dataset.query.filter(Dataset.typology != "specification")
        .order_by(Dataset.name)
        .all()
    )
    form.dataset.choices = [("", "")] + [(d.dataset, d.name) for d in datasets]

    if form.validate_on_submit():
        endpoint_hash = compute_hash(form.endpoint.data)
        endpoint = Endpoint.query.get(endpoint_hash)
        if endpoint:
            session["existing_endpoint"] = endpoint
        session["form_data"] = {
            "endpoint_url": form.endpoint.data,
            "organisation": form.organisation.data,
            "dataset": form.dataset.data,
        }
        session.clear()
        return redirect(url_for("addsource.summary"))
    return render_template("source/create.html", form=form)


@addsource.route("/summary")
def summary():

    # TODO - the session may also contain a key for existing_endpoint
    if session.get("form_data") is not None:
        return render_template("source/summary.html", sources=[session["form_data"]])
    return render_template("source/summary.html")


@addsource.route("/create-mappings")
def mappings():
    return render_template("source/mappings.html")
