from flask import render_template, Blueprint, redirect, url_for

from application.blueprints.addsource.forms import SourceForm
from application.models import Organisation

addsource = Blueprint("addsource", __name__, url_prefix="/add-a-source")


@addsource.route("/", methods=["GET", "POST"])
def index():
    form = SourceForm()
    organisations = Organisation.query.order_by(Organisation.name).all()
    form.organisation.choices = [("", "")] + [
        (o.organisation, o.name) for o in organisations
    ]

    if form.validate_on_submit():
        return redirect(url_for("addsource.mappings"))
    return render_template("source/create.html", form=form)


@addsource.route("/create-mappings")
def mappings():
    return render_template("source/mappings.html")
