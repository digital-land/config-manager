from flask import Blueprint, abort, redirect, render_template, request, url_for

from application.blueprints.dataset.forms import EditRuleForm
from application.db.models import Dataset, PublicationStatus
from application.extensions import db
from application.spec_helpers import (
    PIPELINE_MODELS,
    count_pipeline_rules,
    get_expected_pipeline_specs,
)

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

        form = EditRuleForm()
        form.field.choices = [(field.field, field.field) for field in dataset.fields]
        form.rule_type = rule_type_name
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
        rule = get_rule(rule_id, rule_type_name)
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
