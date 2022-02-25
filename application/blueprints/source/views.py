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
    EditSourceForm,
    NewSourceForm,
    SearchForm,
)
from application.extensions import db
from application.models import Collection, Dataset, Endpoint, Organisation, Source
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
    form.endpoint_url.data = data["endpoint_url"]
    form.organisation.data = data["organisation"]
    # need to change this to work with multiple
    form.dataset.data = ", ".join([dataset["dataset"] for dataset in data["datasets"]])
    form.licence.data = data["licence"]
    form.attribution.data = data["attribution"]
    form.start_date.data = data["start_date"]


def create_source_data(form, _type="new"):
    if _type == "new":
        return {
            "endpoint_url": form.endpoint_url.data,
            "organisation": form.organisation.data,
            "datasets": get_datasets(form.dataset.data),
            "documentation_url": form.documentation_url.data,
            "licence": form.licence.data,
            "attribution": form.attribution.data,
            "start_date": form.start_date.data,
        }
    else:
        return {
            "documentation_url": form.documentation_url.data,
            "licence": form.licence.data,
            "attribution": form.attribution.data,
            "start_date": form.start_date.data,
        }


def create_or_update_endpoint(data):
    endpoint_url = data.pop("endpoint_url").strip()
    hashed_url = compute_hash(endpoint_url)
    endpoint = Endpoint.query.get(hashed_url)
    if endpoint is None:
        endpoint = Endpoint(endpoint=hashed_url, endpoint_url=endpoint_url)
    datasets = data.pop("datasets")
    for dataset in datasets:
        ds = Dataset.query.get(dataset["dataset"])
        collection = Collection.query.get(ds.collection)
        organisation_id = data["organisation"]
        organisation = Organisation.query.get(organisation_id)
        source_key = f"{collection}|{organisation.organisation}|{endpoint.endpoint}"
        source_key = compute_md5_hash(source_key)
        source = Source(
            source=source_key,
            collection=collection,
            organisation=organisation,
        )
        source.update(data)
        ds.sources.append(source)
        endpoint.sources.append(source)
    return endpoint


@source_bp.route("/", methods=["GET", "POST"])
def search():
    form = SearchForm()
    if form.validate_on_submit():
        source_hash = form.source.data.strip()
        source = Source.query.get(source_hash)
        if source:
            return redirect(url_for("source.source", source_hash=source.source))
        form.source.errors.append("We don't recognise that hash, try another")
    sources = (
        Source.query.filter(Source.entry_date != None)  # noqa: E711
        .order_by(Source.entry_date.desc())
        .limit(5)
        .all()
    )
    return render_template("source/search.html", form=form, sources=sources)


@source_bp.route("/add", methods=["GET", "POST"])
def add():
    form = NewSourceForm()
    if request.args.get("_change") and session["form_data"]:
        set_form_values(form, session["form_data"])

    organisations = Organisation.query.order_by(Organisation.name).all()
    form.organisation.choices = [("", "")] + [
        (o.organisation, o.name) for o in organisations
    ]
    datasets = (
        Dataset.query.filter(
            Dataset.typology != "specification", Dataset.collection.is_not(None)
        )
        .order_by(Dataset.name)
        .all()
    )
    form.dataset.choices = [("", "")] + [(d.dataset, d.name) for d in datasets]

    if form.validate_on_submit():
        session["url_reachable"] = check_url_reachable(form.endpoint_url.data.strip())
        endpoint_hash = compute_hash(form.endpoint_url.data.strip())
        endpoint = Endpoint.query.get(endpoint_hash)
        if endpoint is not None:
            # will need to update when/if user can put multiple datasets
            existing_source = endpoint.get_matching_source(
                form.organisation.data, form.dataset.data
            )
            if existing_source is not None:
                session["existing_source"] = existing_source
        session["form_data"] = create_source_data(form)
        return redirect(url_for("source.summary"))
    return render_template("source/create.html", form=form)


@source_bp.route("/add/summary")
def summary():
    url_reachable = session.pop("url_reachable", None)
    return render_template(
        "source/summary.html",
        sources=[session.get("form_data")],
        existing_source=session.get("existing_source"),
        url_reachable=url_reachable,
    )


@source_bp.route("/add/finish")
def finish():
    existing_source = session.pop("existing_source", None)
    form_data = session.pop("form_data", None)
    if not form_data:
        return redirect(url_for("source.add"))
    if existing_source is None:
        endpoint = create_or_update_endpoint(form_data)
        db.session.add(endpoint)
        source = endpoint.sources[-1]
    else:
        source = Source.query.get(existing_source["source"])
        source.update(form_data)
        db.session.add(source)
    db.session.commit()
    return render_template("source/finish.html", source=source)


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
    form = EditSourceForm(obj=source)

    if form.validate_on_submit():
        # if endpoint, org or dataset have changed then has the source changed or is it a new one
        # so ignore those for now
        session["url_reachable"] = True
        session["existing_source"] = source
        session["form_data"] = create_source_data(form, _type="edit")
        return redirect(url_for("source.summary"))

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
