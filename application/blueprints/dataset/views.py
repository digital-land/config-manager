import datetime

from flask import Blueprint, abort, redirect, render_template, request, url_for

from application.blueprints.dataset.forms import EditRuleForm
from application.data_access.overview.digital_land_queries import (
    get_active_resources,
    get_content_type_counts,
    get_datasets,
    get_latest_collector_run_date,
    get_latest_resource,
    get_publisher_coverage,
    get_resource_count_per_dataset,
    get_source_counts,
    get_sources,
    get_themes,
    get_typologies,
)
from application.data_access.overview.entity_queries import get_entity_count
from application.data_access.overview.source_and_resource_queries import (
    get_datasets_summary,
    get_monthly_counts,
    publisher_counts,
)
from application.db.models import Dataset, PublicationStatus
from application.extensions import db
from application.spec_helpers import (
    PIPELINE_MODELS,
    count_pipeline_rules,
    get_expected_pipeline_specs,
)
from application.utils import filter_off_btns, index_by, resources_per_publishers

dataset_bp = Blueprint("dataset", __name__, url_prefix="/dataset")


@dataset_bp.get("/")
def index():
    category_datasets = Dataset.query.filter(
        Dataset.typology_id == "category", Dataset.collection_id.isnot(None)
    ).all()

    datasets = Dataset.query.filter(
        Dataset.typology_id != "category", Dataset.collection_id.isnot(None)
    ).all()

    return render_template(
        "dataset/index.html",
        datasets=datasets,
        category_datasets=category_datasets,
    )


@dataset_bp.get("/<string:dataset_id>")
def dataset(dataset_id):
    dataset = Dataset.query.get(dataset_id)

    if dataset is None or dataset.collection_id is None:
        return abort(404)

    specification_pipelines = get_expected_pipeline_specs()

    rule_counts = count_pipeline_rules(dataset.collection.pipeline)

    return render_template(
        "dataset/dataset.html",
        pipeline=dataset.collection.pipeline,
        dataset=dataset,
        specification_pipelines=specification_pipelines,
        rule_counts=rule_counts,
    )


@dataset_bp.get("/<string:dataset_id>/rules/<string:rule_type_name>")
def rule_type(dataset_id, rule_type_name):
    limited = False
    dataset = Dataset.query.get(dataset_id)

    if dataset is None or dataset.collection_id is None:
        return abort(404)

    # # check if name is one of allowable rule types
    specification_pipelines = get_expected_pipeline_specs()
    if rule_type_name not in specification_pipelines.keys():
        return abort(404)

    rules = getattr(dataset.collection.pipeline, rule_type_name)

    if len(rules) > 1000:
        rules = rules[:1000]
        limited = True

    return render_template(
        "dataset/rules.html",
        dataset=dataset,
        rule_type_name=rule_type_name,
        rule_type_specification=specification_pipelines[rule_type_name],
        rules=rules,
        limited=limited,
    )


@dataset_bp.get("/<string:dataset_id>/sources")
def sources(dataset_id):
    dataset = Dataset.query.get(dataset_id)

    if dataset is None or dataset.collection_id is None:
        return abort(404)

    return render_template(
        "dataset/sources.html",
        dataset=dataset,
    )


def get_rule(id, rule_type):
    return PIPELINE_MODELS[rule_type].query.get(id)


@dataset_bp.get("/<string:dataset_id>/rules/<string:rule_type_name>/<rule_id>")
def edit_rule(dataset_id, rule_type_name, rule_id):
    dataset = Dataset.query.get(dataset_id)

    if dataset is None or dataset.collection_id is None:
        return abort(404)

    specification_pipelines = get_expected_pipeline_specs()
    if rule_type_name not in specification_pipelines.keys():
        return abort(404)

    if rule_id == "new":
        # create empty rule except for dataset

        form = EditRuleForm(dataset_id=dataset.dataset)
        form.field_id.choices = [(field.field, field.field) for field in dataset.fields]
        rule = {"dataset": dataset.dataset}
    else:
        rule = get_rule(rule_id, rule_type_name)
        if rule is None:
            return abort(404)

        form = EditRuleForm(obj=rule, rule_type=rule_type_name)
        if hasattr(form, "field_id"):
            form.field_id.choices = [
                (field.field, field.field) for field in dataset.fields
            ]
            if rule.field:
                form.field_id.data = rule.field.field

    rule_type_specification = specification_pipelines[rule_type_name]
    form_field_names = _get_form_field_names(rule_type_specification)

    return render_template(
        "dataset/editrule.html",
        form=form,
        dataset=dataset,
        rule_type_name=rule_type_name,
        rule_type_specification=rule_type_specification,
        form_field_names=form_field_names,
        rule=rule,
    )


@dataset_bp.post("/<string:dataset_id>/rules/<string:rule_type_name>/<rule_id>")
def save_rule(dataset_id, rule_type_name, rule_id):
    form = EditRuleForm(request.form)
    if form.validate_on_submit():
        dataset = Dataset.query.get(dataset_id)
        if dataset is None:
            return abort(404)

        if rule_id == "new":
            rule_class = PIPELINE_MODELS[rule_type_name]
            rule = rule_class()
            rule.pipeline = dataset.collection.pipeline
        else:
            rule = get_rule(rule_id, rule_type_name)
        if rule is None:
            return abort(404)

        form.populate_obj(rule)
        rule.pipeline.publication_status = PublicationStatus.DRAFT.name
        db.session.add(rule)
        db.session.commit()
        return redirect(
            url_for(
                "dataset.rule_type",
                dataset_id=dataset_id,
                rule_type_name=rule_type_name,
            )
        )
    return render_template("dataset/editrule.html", form=form)


def _get_form_field_names(rule_type_specification):
    field_names = []
    for f in rule_type_specification.fields:
        if f.field == rule_type_specification.dataset:
            field_names.append(f.field)
        elif f.field in ["dataset", "endpoint", "field"]:
            field_names.append(f"{f.field}_id")
        else:
            field_names.append(f.field.replace("-", "_"))
    return field_names


@dataset_bp.route("/overview/<dataset>")
def dataset_overview(dataset):
    datasets = get_datasets_summary()
    dataset_name = dataset
    dataset = [v for k, v in datasets.items() if v.get("pipeline") == dataset]

    resources_by_publisher = resources_per_publishers(
        get_active_resources(dataset_name)
    )

    # publishers = fetch_publisher_stats(dataset_name)
    publishers = publisher_counts(dataset_name)
    publisher_splits = {"active": [], "noactive": []}
    for k, publisher in publishers.items():
        if publisher["active_resources"] == 0:
            publisher_splits["noactive"].append(publisher)
        else:
            publisher_splits["active"].append(publisher)

    # for the active resource charts
    resource_stats = {
        "over_one": len(
            [p for p in resources_by_publisher if len(resources_by_publisher[p]) > 1]
        ),
        "one": len(
            [p for p in resources_by_publisher if len(resources_by_publisher[p]) == 1]
        ),
        "zero": len(publisher_splits["noactive"]),
    }
    resource_counts = index_by("pipeline", get_resource_count_per_dataset())

    resource_count = (
        resource_counts[dataset_name]["resources"]
        if resource_counts.get(dataset_name)
        else 0
    )

    sources_no_doc_url, query_url = get_sources(
        limit=500, filter={"pipeline": dataset_name}
    )

    try:
        # wrapping in try/except because datasette occasionally timesout
        content_type_counts = sorted(
            get_content_type_counts(dataset=dataset_name),
            key=lambda x: x["resource_count"],
            reverse=True,
        )
    except Exception as e:
        print(e)
        content_type_counts = []

    blank_sources, bls_query = get_sources(
        limit=500,
        filter={"pipeline": dataset_name},
        only_blanks=True,
    )

    return render_template(
        "dataset/performance.html",
        name=dataset_name,
        dataset=dataset[0] if len(dataset) else "",
        latest_resource=get_latest_resource(dataset_name),
        monthly_counts=get_monthly_counts(pipeline=dataset_name),
        publishers=publisher_splits,
        today=datetime.datetime.utcnow().isoformat()[:10],
        entity_count=get_entity_count(pipeline=dataset_name),
        resource_count=resource_count,
        coverage=get_publisher_coverage(dataset_name),
        resource_stats=resource_stats,
        sources_no_doc_url=sources_no_doc_url,
        content_type_counts=content_type_counts,
        latest_logs=get_latest_collector_run_date(dataset=dataset_name),
        blank_sources=blank_sources,
        source_count=get_source_counts(pipeline=dataset_name),
    )


@dataset_bp.route("/overview")
def datasets():
    filters = {}
    # if request.args.get("active"):
    #     filters["active"] = request.args.get("active")
    if request.args.get("theme"):
        filters["theme"] = request.args.get("theme")
    if request.args.get("typology"):
        filters["typology"] = request.args.get("typology")

    if len(filters.keys()):
        dataset_records = get_datasets(filter=filters)
    else:
        dataset_records = get_datasets()

    return render_template(
        "dataset/index.html",
        datasets=dataset_records,
        filters=filters,
        filter_btns=filter_off_btns(filters),
        themes=get_themes(),
        typologies=get_typologies(),
    )
