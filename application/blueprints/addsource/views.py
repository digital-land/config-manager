from flask import render_template, Blueprint, redirect, url_for, session

from application.blueprints.addsource.forms import SourceForm
from application.models import Organisation, Dataset

addsource = Blueprint("addsource", __name__, url_prefix="/add-a-source")


@addsource.route("/", methods=["GET", "POST"])
def index():
    form = SourceForm()
    organisations = Organisation.query.order_by(Organisation.name).all()
    form.organisation.choices = [("", "")] + [
        (o.organisation, o.name) for o in organisations
    ]
    datasets = Dataset.query.order_by(Dataset.name).all()
    # To Do: remove the config datasets from list e.g. field-name
    form.dataset.choices = [("", "")] + [(d.dataset, d.name) for d in datasets]

    if form.validate_on_submit():
        session["form_data"] = {
            "endpoint_url": form.endpoint.data,
            "organisation": form.organisation.data,
            "dataset": form.dataset.data,
        }
        return redirect(url_for("addsource.summary"))
    return render_template("source/create.html", form=form)


@addsource.route("/summary")
def summary():
    if session["form_data"] is not None:
        return render_template("source/summary.html", sources=[session["form_data"]])
    return render_template("source/summary.html")


@addsource.route("/create-mappings")
def mappings():
    return render_template("source/mappings.html")
