from flask import (
    Blueprint,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    url_for,
)

from application.blueprints.source.forms import ArchiveForm, SearchForm, SourceForm
from application.extensions import db
from application.models import Dataset, Endpoint, Organisation, Source
from application.utils import check_url_reachable, compute_hash, compute_md5_hash

source_bp = Blueprint("source", __name__, url_prefix="/source")


def organisation_choices():
    organisations = Organisation.query.order_by(Organisation.name).all()
    return [("", "")] + [(o.organisation, o.name) for o in organisations]


def dataset_choices():
    datasets = (
        Dataset.query.filter(Dataset.typology != "specification")
        .order_by(Dataset.name)
        .all()
    )
    return [("", "")] + [(d.dataset, d.name) for d in datasets]


def get_datasets(s, sep=","):
    ids = s.split(sep)
    return Dataset.query.filter(Dataset.dataset.in_(ids)).all()


def set_form_values(form, data):
    form.endpoint.data = data["endpoint_url"]
    form.organisation.data = data["organisation"]
    # need to change this to work with multiple
    form.dataset.data = data["datasets"][0]["dataset"]
    form.licence.data = data["licence"]
    form.attribution.data = data["attribution"]
    form.start_date.data = data["start_date"]


def create_source_obj(form):
    return {
        "endpoint_url": form.endpoint.data,
        "organisation": form.organisation.data,
        "datasets": get_datasets(form.dataset.data),
        "documentation_url": form.documentation_url.data,
        "licence": form.licence.data,
        "attribution": form.attribution.data,
        "start_date": form.start_date.data,
    }


@source_bp.route("/", methods=["GET", "POST"])
def search():
    form = SearchForm()
    if form.validate_on_submit():
        source_hash = form.source.data
        source = Source.query.get(source_hash)
        if source:
            return redirect(url_for("source.source", source_hash=source.source))
        form.source.errors.append("We don't recognise that hash, try another")
    return render_template("source/search.html", form=form)


@source_bp.route("/add", methods=["GET", "POST"])
def add():
    form = SourceForm()
    if request.args.get("_change") and session["form_data"]:
        set_form_values(form, session["form_data"])

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
        session["existing_endpoint"] = None
        session["existing_source"] = None

        session["url_reachable"] = check_url_reachable(form.endpoint.data)
        endpoint_hash = compute_hash(form.endpoint.data)
        endpoint = Endpoint.query.get(endpoint_hash)
        if endpoint is not None:
            session["existing_endpoint"] = endpoint

            existing_source = endpoint.get_matching_source(
                form.organisation.data, form.dataset.data
            )
            if existing_source is not None:
                session["existing_source"] = existing_source

        session["form_data"] = create_source_obj(form)
        session["form_data"] = {
            "endpoint_url": form.endpoint.data,
            "organisation": form.organisation.data,
            "datasets": [form.dataset.data],
            "licence": form.licence.data,
            "attribution": form.attribution.data,
            "start_date": form.start_date.data,
        }

        return redirect(url_for("source.summary"))
    return render_template("source/create.html", form=form)


@source_bp.route("/add/summary")
def summary():
    # if the source already exists then let user choose to edit it
    # TODO - the session may also contain a key for existing_endpoint
    # at some point pop the relevant keys from session
    return render_template(
        "source/summary.html",
        sources=[session.get("form_data")],
        existing_source=session.get("existing_source"),
        url_reachable=session["url_reachable"],
    )


@source_bp.route("/add/finish")
def finish():
    # To do: save the new source or update existing source
    existing_source = session.pop("existing_source", None)
    existing_endpoint = session.pop("existing_endpoint", None)
    form_data = session.pop("form_data", None)
    if not form_data:
        return redirect(url_for("source.add"))
    if existing_source is None and existing_endpoint is None:
        endpoint_url = form_data.pop("endpoint_url")
        hashed_url = compute_hash(endpoint_url)
        endpoint = Endpoint(endpoint=hashed_url, endpoint_url=endpoint_url)
        dataset_id = form_data.pop("datasets")[0]
        dataset = Dataset.query.get(dataset_id)
        collection = dataset_id
        organisation_id = form_data["organisation"]
        organisation = Organisation.query.get(organisation_id)
        source_key = f"{collection}|{organisation.organisation}|{endpoint.endpoint}"
        source_key = compute_md5_hash(source_key)
        source = Source(
            source=source_key,
            collection=collection,
            organisation=organisation,
            datasets=[dataset],
        )
        endpoint.sources.append(source)
        db.session.add(endpoint)
        db.session.commit()

    return render_template("source/finish.html")


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


@source_bp.route("<source_hash>/edit", methods=["GET", "POST"])
def edit(source_hash):
    source = Source.query.get(source_hash)
    form = SourceForm(obj=source)
    form.organisation.choices = organisation_choices()
    form.dataset.choices = dataset_choices()
    if form.validate_on_submit():
        # if endpoint, org or dataset have changed then has the source changed or is it a new one
        # so ignore those for now
        session["url_reachable"] = True
        session["existing_source"] = source
        session["form_data"] = create_source_obj(form)
        return redirect(url_for("source.summary"))

    # add default values
    form.organisation.data = source.organisation.organisation
    # to do: handle multiple datasets
    form.dataset.data = source.datasets[0].dataset
    form.endpoint.data = source.endpoint.endpoint_url

    cancel_href = url_for("source.source", source_hash=source.source)
    if url_for("source.summary") in request.referrer:
        cancel_href = request.referrer
    return render_template(
        "source/edit.html", source=source, form=form, cancel_href=cancel_href
    )


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
