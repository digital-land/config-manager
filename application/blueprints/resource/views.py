import collections
import os
import tempfile

import requests
from digital_land.api import DigitalLandApi
from flask import (
    Blueprint,
    abort,
    current_app,
    jsonify,
    redirect,
    render_template,
    request,
    url_for,
)

from application.blueprints.resource.forms import MappingForm, SearchForm
from application.collection_utils import Workspace, convert_and_truncate_resource
from application.extensions import db
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
    # has local check been performed?
    check = SourceCheck.query.filter(SourceCheck.resource_hash == resource_hash).first()
    return render_template("resource/resource.html", resource=resource, check=check)


@resource_bp.route("/<resource_hash>/check")
def resource_check(resource_hash):

    resource = Resource.query.get(resource_hash)
    if resource is None:
        return abort(404)

    source = resource.endpoints[0].sources[0]
    dataset = source.datasets[0]
    expected_fields = [field.field for field in dataset.fields]

    # if already checked return from db
    source_check = SourceCheck.query.filter(
        SourceCheck.resource_hash == resource_hash
    ).first()
    if source_check is not None:
        return jsonify(
            {
                "resource_fields": source_check.resource_fields,
                "expected_fields": expected_fields,
                "resource_rows": source_check.resource_rows,
            }
        )

    with tempfile.TemporaryDirectory() as temp_dir:
        workspace = Workspace.factory(
            source, dataset, temp_dir, current_app.config["PROJECT_ROOT"]
        )
        api = DigitalLandApi(
            False, dataset, workspace.pipeline_dir, workspace.specification_dir
        )
        bucket_url = current_app.config["S3_BUCKET_URL"]
        resource_url = f"{bucket_url}/{dataset.collection}-collection/collection/resource/{resource_hash}"

        try:
            resp = requests.get(resource_url)
            resp.raise_for_status()
        except requests.HTTPError as e:
            print(e)
            return abort(404)

        content = resp.content.decode("utf-8")
        resource_path = os.path.join(workspace.resource_dir, resource_hash)
        with open(resource_path, "w") as f:
            f.write(content)
        limit = int(request.args.get("limit")) if request.args.get("limit") else 10
        (
            resource_fields,
            input_path,
            output_path,
            resource_rows,
        ) = convert_and_truncate_resource(api, workspace, resource_hash, limit)

        source.check = SourceCheck(
            resource_hash=resource_hash,
            resource_rows=resource_rows,
            resource_fields=resource_fields,
        )
        db.session.add(source)
        db.session.commit()

    return jsonify(
        {
            "resource_fields": resource_fields,
            "expected_fields": expected_fields,
            "resource_rows": resource_rows,
        }
    )


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


def remove_already_mapped(fields, mappings, type_="field"):
    # mappings are list of mapping_objs
    mapping_fields = [
        (mapping.field_id if type_ == "field" else mapping.column)
        for mapping in mappings
    ]
    return [field for field in fields if field not in mapping_fields]


def outstanding_fields(
    expected_fields, resource_columns, relevant_dataset_mappings, resource_mappings
):
    unmatched = [field for field in expected_fields if field not in resource_columns]
    for mappings in [relevant_dataset_mappings, resource_mappings]:
        unmatched = remove_already_mapped(unmatched, mappings)

    return unmatched


def remaining_columns(
    resource_columns, expected_fields, relevant_dataset_mappings, resource_mappings
):
    unmatched = [column for column in resource_columns if column not in expected_fields]
    for mappings in [relevant_dataset_mappings, resource_mappings]:
        unmatched = remove_already_mapped(unmatched, mappings, type_="column")
    return unmatched


def get_dataset_from_param(request, datasets):
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
    return dataset_obj


@resource_bp.route("/<resource_hash>/columns")
def columns(resource_hash):
    resource = Resource.query.get(resource_hash)
    datasets = get_resource_datasets(resource)
    dataset_obj = get_dataset_from_param(request, datasets)

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

    relevant_dataset_mappings = relevant_mappings(
        dataset_mappings, summary.resource_fields
    )

    expected_fields = [field.field for field in dataset_obj.fields]
    missing_fields = outstanding_fields(
        expected_fields,
        summary.resource_fields,
        relevant_dataset_mappings,
        resource_mappings,
    )
    unused_columns = remaining_columns(
        summary.resource_fields,
        expected_fields,
        relevant_dataset_mappings,
        resource_mappings,
    )

    # To do: get columns/attr names from the original resource
    # To do: get expected/allowable attributes from schema
    # To do: get mappings between columns and expected columns
    # To do: link to somewhere to edit mappings
    return render_template(
        "resource/columns.html",
        resource=resource,
        datasets=datasets,
        dataset=dataset_obj,
        relevant_dataset_mappings=relevant_dataset_mappings,
        resource_mappings=resource_mappings,
        expected_fields=expected_fields,
        summary=summary,
        sample_row=collections.OrderedDict(sorted(summary.resource_rows[0].items())),
        missing_fields=missing_fields,
        unused_columns=unused_columns,
    )


@resource_bp.route("/<resource_hash>/columns/add", methods=["GET", "POST"])
def columns_add(resource_hash):
    form = MappingForm()
    resource = Resource.query.get(resource_hash)
    datasets = get_resource_datasets(resource)
    dataset_obj = get_dataset_from_param(request, datasets)

    summary = SourceCheck.query.filter(
        SourceCheck.resource_hash == resource.resource
    ).first()
    if summary is None:
        # perform the /check
        pass

    expected_fields = [field.field for field in dataset_obj.fields]
    resource_columns = summary.resource_fields

    form.column.choices = [("", "")] + sorted([(col, col) for col in resource_columns])
    form.field.choices = [("", "")] + sorted(
        [(field, field) for field in expected_fields]
    )

    if form.validate_on_submit():
        # to do
        # save the new rule before redirecting to page with all rules listed
        mapping = Column(
            column=form.column.data,
            dataset_id=dataset_obj.dataset,
            field_id=form.field.data,
            resource_id=resource_hash,
        )
        db.session.add(mapping)
        db.session.commit()
        return redirect(url_for("resource.columns", resource_hash=resource_hash))

    return render_template(
        "resource/column-add.html",
        resource=resource,
        form=form,
        dataset=dataset_obj,
        sample_row=collections.OrderedDict(sorted(summary.resource_rows[0].items())),
        expected_fields=[field.field for field in dataset_obj.fields],
        resource_columns=summary.resource_fields,
    )


# should the urls be /<resource_hash>/dataset/<dataset>/columns/{,remove,add}?
@resource_bp.route("/<resource_hash>/columns/remove")
def columns_remove(resource_hash):
    if (
        request.args.get("dataset") is None
        or request.args.get("column") is None
        or request.args.get("field") is None
    ):
        # should fail more nicely than this
        return abort(404)
    dataset = request.args.get("dataset")
    column = request.args.get("column")
    field = request.args.get("field")

    mapping = Column.query.filter(
        Column.dataset_id == dataset,
        Column.column == column,
        Column.field_id == field,
        Column.resource_id == resource_hash,
    ).first()

    if mapping:
        db.session.delete(mapping)
        db.session.commit()

    return redirect(
        url_for("resource.columns", resource_hash=resource_hash, dataset=dataset)
    )


@resource_bp.route("/<resource_hash>/values")
def values(resource_hash):
    # To do: get expected/allowable attributes from schema and check if any contain specific values (category fields)
    # To do: get allowable values
    # To do: check values in resource against allowable values
    # To do: link to somewhere to edit mappings
    return render_template("resource/values.html")
