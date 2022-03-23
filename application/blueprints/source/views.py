import os
import tempfile
from datetime import datetime

from digital_land.api import DigitalLandApi
from flask import (
    Blueprint,
    abort,
    current_app,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    send_file,
    url_for,
)

from application.blueprints.source.forms import (
    ArchiveForm,
    EditSourceForm,
    NewSourceForm,
    SearchForm,
)
from application.collection_utils import (
    convert_and_truncate_resource,
    workspace_factory,
)
from application.extensions import db
from application.models import (
    Collection,
    Dataset,
    Endpoint,
    Organisation,
    Source,
    SourceCheck,
)
from application.utils import (
    check_url_reachable,
    compute_hash,
    compute_md5_hash,
    csv_data_to_buffer,
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
    dataset = data.get("dataset")
    ds = Dataset.query.get(dataset)
    collection = Collection.query.get(ds.collection)
    organisation_id = data.get("organisation")
    organisation = Organisation.query.get(organisation_id)
    source_key = f"{collection}|{organisation.organisation}|{endpoint.endpoint}"
    source_key = compute_md5_hash(source_key)
    # check if source exists - can happen if user refreshes finish page
    source = Source.query.get(source_key)
    if source is None:
        source = Source(
            source=source_key,
            collection=collection,
            organisation=organisation,
            entry_date=datetime.now().isoformat(),
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
        Dataset.query.filter(
            Dataset.typology != "specification", Dataset.collection.is_not(None)
        )
        .order_by(Dataset.name)
        .all()
    )
    form.dataset.choices = [("", "")] + [(d.dataset, d.name) for d in datasets]

    if request.args and not request.args.get("_change") and form.validate():
        endpoint_hash = compute_hash(form.endpoint_url.data.strip())
        endpoint = Endpoint.query.get(endpoint_hash)
        url_reachable = check_url_reachable(form.endpoint_url.data.strip())
        query_params = {"url_reachable": url_reachable, **request.args}
        if endpoint is not None:
            # will need to update when/if user can put multiple datasets
            existing_source = endpoint.get_matching_source(
                form.organisation.data, form.dataset.data
            )
            if existing_source is not None:
                query_params["existing_source"] = existing_source

        return redirect(url_for("source.summary", **query_params))

    return render_template("source/create.html", form=form)


@source_bp.get("/add/summary")
def summary():
    form = NewSourceForm(request.args)
    url_reachable = request.args.get("url_reachable", None)
    existing_source_id = request.args.get("existing_source", None)
    existing_source = (
        Source.query.get(existing_source_id) if existing_source_id is not None else None
    )
    dataset = Dataset.query.get(form.dataset.data)
    organisation = Organisation.query.get(form.organisation.data)
    return render_template(
        "source/summary.html",
        sources=[form.data],
        existing_source=existing_source,
        url_reachable=url_reachable,
        organisation=organisation,
        dataset=dataset,
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


@source_bp.route("<source_hash>.json")
def source_json(source_hash):
    source = Source.query.get(source_hash)
    if source:
        return jsonify(source), 200
    return {}, 404


@source_bp.route("<source_hash>/edit", methods=["GET", "POST"])
@login_required
def edit(source_hash):
    source = Source.query.get(source_hash)
    form = EditSourceForm(obj=source)

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


@source_bp.route("/create-mappings")
def mappings():
    return render_template("source/mappings.html")


@source_bp.route("/<source_hash>/<filename>.csv")
def source_csv(source_hash, filename):
    source = Source.query.get(source_hash)
    if source is None:
        return abort(404)

    csv_rows = []
    if filename == "source":
        items = source.collection.sources
    elif filename == "endpoint":
        seen = set([])
        items = []
        for s in source.collection.sources:
            if s.endpoint is not None and s.endpoint.endpoint not in seen:
                items.append(s.endpoint)
                seen.add(s.endpoint.endpoint)
    else:
        abort(404)

    for item in items:
        try:
            csv_rows.append(item.to_csv_dict())
        except Exception as e:
            print(e)

    buffer = csv_data_to_buffer(csv_rows)
    return send_file(
        buffer,
        as_attachment=True,
        attachment_filename=f"{filename}.csv",
        mimetype="text/csv",
    )


@source_bp.get("/<source_hash>/check")
@login_required
def source_check(source_hash):
    source_obj = Source.query.get(source_hash)
    if source_obj is None:
        return abort(404)

    # if there is a request arg for dataset we can then find the correct
    # one to use rather than just take the first dataset from source_obj.datasets
    if len(source_obj.datasets) > 1 and not request.args.get("dataset"):
        flash("This source has has more than one dataset")
        return redirect(url_for("source.source"))

    dataset_obj = source_obj.datasets[0]
    dataset = dataset_obj.dataset
    expected_fields = [field.field for field in dataset_obj.fields]

    # if already checked return from db
    if source_obj.check is not None:
        return jsonify(
            {
                "resource_fields": source_obj.check.resource_fields,
                "expected_fields": expected_fields,
                "resource_rows": source_obj.check.resource_rows,
            }
        )

    # else fetch from url, convert and truncate
    with tempfile.TemporaryDirectory() as temp_dir:

        workspace = workspace_factory(
            source_obj, dataset_obj, temp_dir, current_app.config["PROJECT_ROOT"]
        )
        api = DigitalLandApi(
            False, dataset, workspace.pipeline_dir, workspace.specification_dir
        )

        api.collect_cmd(workspace.endpoint_csv, workspace.collection_dir)

        resources = os.listdir(workspace.resource_dir)

        if not resources:
            print("No resource collected")
            return abort(400)
        else:
            resource_hash = resources[0]
            limit = int(request.args.get("limit")) if request.args.get("limit") else 10
            (
                resource_fields,
                input_path,
                output_path,
                resource_rows,
            ) = convert_and_truncate_resource(api, workspace, resource_hash, limit)

        source_obj.check = SourceCheck(
            resource_hash=resource_hash,
            resource_rows=resource_rows,
            resource_fields=resource_fields,
        )
        db.session.add(source_obj)
        db.session.commit()

        return jsonify(
            {
                "resource_fields": resource_fields,
                "expected_fields": expected_fields,
                "resource_rows": resource_rows,
            }
        )
