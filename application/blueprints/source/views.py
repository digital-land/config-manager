from flask import (
    Blueprint,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    url_for,
)

from application.blueprints.source.forms import (
    ArchiveForm,
    NewSourceForm,
    SearchForm,
    SourceForm,
)
from application.models import Dataset, Endpoint, Organisation, Source
from application.utils import compute_hash

source_bp = Blueprint("source", __name__, url_prefix="/source")


def organisation_choices():
    organisations = Organisation.query.order_by(Organisation.name).all()
    return [("", "")] + [(o.organisation, o.name) for o in organisations]


@source_bp.route("/add", methods=["GET", "POST"])
def add():
    form = NewSourceForm()
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
        # check if source already exists
        source = Source.query.filter(
            Source._endpoint == endpoint.endpoint,
            Source._organisation == form.organisation.data,
        ).all()
        if source:
            session["existing_source"] = source
        return redirect(url_for("source.summary"))
    return render_template("source/create.html", form=form)


@source_bp.route("/add/summary")
def summary():

    # TODO - the session may also contain a key for existing_endpoint
    if session.get("form_data") is not None:
        return render_template("source/summary.html", sources=[session["form_data"]])
    return render_template("source/summary.html")


@source_bp.route("/add/finish")
def finish():
    return render_template("source/finish.html")


@source_bp.route("source", methods=["GET", "POST"])
def search():
    form = SearchForm()
    if form.validate_on_submit():
        source_hash = form.source.data
        source = Source.query.get(source_hash)
        if source:
            return redirect(url_for("source.edit", source_hash=source.source))
        form.source.errors.append("We don't recognise that hash, try another")
    return render_template("source/search.html", form=form)


@source_bp.route("<source_hash>")
def source(source_hash):
    source = Source.query.get(source_hash)
    return render_template("source/source.html", source=source)


@source_bp.route("<source_hash>.json")
def source_json(source_hash):
    source = Source.query.get(source_hash)
    if source:
        return jsonify(source), 200
    return {}, 404


@source_bp.route("<source_hash>/edit")
def edit(source_hash):
    source = Source.query.get(source_hash)
    form = SourceForm(obj=source)
    form.organisation.choices = organisation_choices()
    form.organisation.data = source.organisation.organisation
    form.endpoint.data = source.endpoint.endpoint_url
    return render_template("source/edit.html", source=source, form=form)


@source_bp.route("<source_hash>/archive")
def archive(source_hash):
    source = Source.query.get(source_hash)
    form = ArchiveForm()
    if form.validate_on_submit():
        # do something with the answer
        pass
    return render_template(
        "source/archive.html", source=source, form=form, referrer=request.referrer
    )


@source_bp.route("/create-mappings")
def mappings():
    return render_template("source/mappings.html")
