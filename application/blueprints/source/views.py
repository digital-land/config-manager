from datetime import datetime

# from digital_land.api import DigitalLandApi
from flask import Blueprint, redirect, render_template, request, url_for

from application.blueprints.source.forms import (
    ArchiveForm,
    EditSourceForm,
    NewSourceForm,
    SearchForm,
)
from application.db.models import (
    Attribution,
    Dataset,
    Endpoint,
    Licence,
    Organisation,
    Source,
)
from application.extensions import db
from application.utils import (
    check_url_reachable,
    compute_hash,
    compute_md5_hash,
    login_required,
)

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


def create_source_data(form, _type="new"):
    if _type == "new":
        return {
            "endpoint_url": form.endpoint_url.data,
            "organisation": form.organisation.data,
            "dataset": form.dataset.data,
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


def dataset_str_to_objs(s):
    source_datasets = s.split(";")
    return Dataset.query.filter(Dataset.dataset.in_(source_datasets)).all()


def create_or_update_endpoint(data):
    endpoint_url = data.get("endpoint_url").strip()
    hashed_url = compute_hash(endpoint_url)
    endpoint = Endpoint.query.get(hashed_url)
    if endpoint is None:
        endpoint = Endpoint(
            endpoint=hashed_url,
            endpoint_url=endpoint_url,
            entry_date=datetime.now().isoformat(),
        )
    datasets = Dataset.query.filter(
        Dataset.dataset.in_(data.get("dataset").split(";"))
    ).all()
    organisation = Organisation.query.get(data.get("organisation"))
    collection = datasets[0].collection
    source_key = (
        f"{collection.collection}|{organisation.organisation}|{endpoint.endpoint}"
    )
    source_key = compute_md5_hash(source_key)
    # check if source exists - can happen if user refreshes finish page
    source = Source.query.get(source_key)
    if source is None:
        source = Source(
            source=source_key,
            organisation=organisation,
            entry_date=datetime.now().isoformat(),
            collection=collection,
            datasets=datasets,
        )
    source.update(data)
    db.session.add(source)
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
        .limit(10)
        .all()
    )
    return render_template("source/search.html", form=form, sources=sources)


def clean_dataset_string(s):
    ds = s.replace(",", ";").split(";")
    return ";".join([d.strip() for d in ds])


@source_bp.get("/add")
@login_required
def add():
    if request.args:
        form = NewSourceForm(request.args)
    else:
        form = NewSourceForm()

    organisations = Organisation.query.order_by(Organisation.name).all()
    form.organisation.choices = [("", "")] + [
        (o.organisation, o.name) for o in organisations
    ]
    datasets = (
        Dataset.query.filter(Dataset.typology_id != "specification")
        .order_by(Dataset.name)
        .all()
    )
    form.attribution.choices = [("", "")] + [
        (attribution.attribution, attribution.attribution)
        for attribution in Attribution.query.all()
    ]
    form.licence.choices = [("", "")] + [
        (licence.licence, licence.text) for licence in Licence.query.all()
    ]

    if request.args and not request.args.get("_change") and form.validate():
        endpoint_hash = compute_hash(form.endpoint_url.data.strip())
        endpoint = Endpoint.query.get(endpoint_hash)
        url_reachable = check_url_reachable(form.endpoint_url.data.strip())
        source_params = {
            **request.args,
            **{"dataset": clean_dataset_string(request.args.get("dataset"))},
        }
        query_params = {"url_reachable": url_reachable, **source_params}
        if endpoint is not None:
            # will need to update when/if user can put multiple datasets
            existing_source = endpoint.get_matching_source(
                form.organisation.data, form.dataset.data
            )
            if existing_source is not None:
                query_params["existing_source"] = existing_source

        return redirect(url_for("source.summary", **query_params))

    return render_template("source/create.html", form=form, datasets=datasets)


@source_bp.get("/add/summary")
def summary():
    form = NewSourceForm(request.args)
    url_reachable = request.args.get("url_reachable", None)
    existing_source_id = request.args.get("existing_source", None)
    existing_source = (
        Source.query.get(existing_source_id) if existing_source_id is not None else None
    )
    if existing_source:
        datasets = existing_source.datasets
        organisation = existing_source.organisation
    else:
        datasets = dataset_str_to_objs(form.dataset.data)
        organisation = Organisation.query.get(form.organisation.data)
    return render_template(
        "source/summary.html",
        sources=[form.data],
        existing_source=existing_source,
        url_reachable=url_reachable,
        organisation=organisation,
        datasets=datasets,
        form=form,
    )


@source_bp.get("/add/finish")
@login_required
def finish():
    # user is not at the end of the add a source journey
    if len(request.args) == 0:
        return redirect(url_for("source.add"))

    form = NewSourceForm(request.args)
    existing_source = request.args.get("existing_source", None)
    if existing_source is None:
        endpoint = create_or_update_endpoint(form.data)
        db.session.add(endpoint)
        source = endpoint.sources[-1]
    else:
        source = Source.query.get(existing_source)
        source.update(form.data)
        db.session.add(source)
    db.session.commit()
    return render_template("source/finish.html", source=source)


@source_bp.route("<source_hash>")
def source(source_hash):
    source = Source.query.get(source_hash)
    return render_template("source/source.html", source=source)


@source_bp.route("<source_hash>/edit", methods=["GET", "POST"])
@login_required
def edit(source_hash):
    source = Source.query.get(source_hash)
    form = EditSourceForm(obj=source)
    form.licence.choices = [
        (licence.licence, licence.text) for licence in Licence.query.all()
    ]
    form.attribution.choices = [
        (attribution.attribution, attribution.text)
        for attribution in Attribution.query.all()
    ]

    if form.validate_on_submit():
        # if endpoint, org or dataset have changed then has the source changed or is it a new one
        # so ignore those for now
        params = create_source_data(form, _type="edit")
        params["existing_source"] = source.source
        params["url_reachable"] = True
        return redirect(url_for("source.summary", **params))

    cancel_href = url_for("source.source", source_hash=source.source)
    if request.referrer and url_for("source.summary") in request.referrer:
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
