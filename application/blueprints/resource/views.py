from flask import Blueprint, abort, jsonify, redirect, render_template, request, url_for

from application.blueprints.resource.forms import MappingForm, SearchForm
from application.models import Column, Resource, SourceCheck

resource_bp = Blueprint("resource", __name__, url_prefix="/resource")


@resource_bp.route("/", methods=["GET", "POST"])
def search():
    form = SearchForm()
    if form.validate_on_submit():
        resource_hash = form.resource.data.strip()
        resource = Resource.query.get(resource_hash)
        if resource:
            return redirect(
                url_for("resource.resource", resource_hash=resource.resource)
            )
        form.resource.errors.append("We don't recognise that hash, try another")

    resources = (
        Resource.query.filter(Resource.start_date != None)  # noqa: E711
        .order_by(Resource.start_date.desc())
        .limit(5)
        .all()
    )

    return render_template("resource/search.html", form=form, resources=resources)


@resource_bp.route("/rules")
def rules():
    return render_template("resource/rules.html")


@resource_bp.route("/<resource_hash>")
def resource(resource_hash):
    resource = Resource.query.get(resource_hash)
    return render_template("resource/resource.html", resource=resource)


@resource_bp.route("/<resource_hash>.json")
def resource_json(resource_hash):
    resource = Resource.query.get(resource_hash)
    if resource:
        return jsonify(resource), 200
    return {}, 404


def get_resource_datasets(resource):
    datasets = []
    for ep in resource.endpoints:
        for source in ep.sources:
            for dataset in source.datasets:
                datasets.append(dataset)
    return list(set(datasets))


def relevant_mappings(dataset_mappings, resource_columns):
    return [
        mapping for mapping in dataset_mappings if mapping.column in resource_columns
    ]


@resource_bp.route("/<resource_hash>/columns")
def columns(resource_hash):
    resource = Resource.query.get(resource_hash)
    datasets = get_resource_datasets(resource)

    # default to first dataset of list
    dataset_obj = datasets[0]
    if request.args.get("dataset") is not None:
        # check it is a relevant dataset for the resource
        if request.args.get("dataset") not in [dataset.dataset for dataset in datasets]:
            # should tell user that resource isn't part of dataset
            return abort(404)
        dataset_obj = next(
            d for d in datasets if request.args.get("dataset") == d.dataset
        )

    # getting exisiting mappings - ignore any that have end-dates
    existing_mappings = (
        Column.query.filter(
            Column.dataset_id == dataset_obj.dataset, Column.end_date.is_(None)
        )
        .order_by(Column.field_id)
        .all()
    )
    dataset_mappings = [
        mapping for mapping in existing_mappings if mapping.resource is None
    ]
    resource_mappings = [
        mapping
        for mapping in existing_mappings
        if mapping.resource and mapping.resource.resource == resource.resource
    ]

    summary = SourceCheck.query.filter(
        SourceCheck.resource_hash == resource.resource
    ).first()
    if summary is None:
        # perform the /check
        pass

    # To do: get columns/attr names from the original resource
    # To do: get expected/allowable attributes from schema
    # To do: get mappings between columns and expected columns
    # To do: link to somewhere to edit mappings
    return render_template(
        "resource/columns.html",
        resource=resource,
        datasets=datasets,
        relevant_dataset_mappings=relevant_mappings(
            dataset_mappings, summary.resource_fields
        ),
        resource_mappings=resource_mappings,
        expected_fields=[field.field for field in dataset_obj.fields],
    )


@resource_bp.route("/<resource_hash>/columns/add", methods=["GET", "POST"])
def columns_add(resource_hash):
    form = MappingForm()
    resource = Resource.query.get(resource_hash)

    if form.validate_on_submit():
        return redirect(url_for("resource.columns", resource_hash=resource_hash))

    return render_template(
        "resource/column-add.html",
        resource=resource,
        form=form,
        sample_row={},
        missing_fields=[],
        available_columns=[],
    )


@resource_bp.route("/<resource_hash>/values")
def values(resource_hash):
    # To do: get expected/allowable attributes from schema and check if any contain specific values (category fields)
    # To do: get allowable values
    # To do: check values in resource against allowable values
    # To do: link to somewhere to edit mappings
    return render_template("resource/values.html")
