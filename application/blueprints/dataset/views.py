from flask import Blueprint, abort, render_template

from application.db.models import Dataset
from application.export.models import PipelineModel
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

    pipeline = PipelineModel.from_orm(dataset.collection.pipeline).dict(by_alias=True)

    return render_template(
        "dataset/dataset.html",
        pipeline=pipeline,
        dataset=dataset,
        specification_pipelines=specification_pipelines,
        rule_counts=rule_counts,
    )


@dataset_bp.get("/<string:dataset_id>/rules/<string:ruletype_name>")
def ruletype(dataset_id, ruletype_name):
    limited = False
    dataset = Dataset.query.get(dataset_id)

    if dataset is None or dataset.collection_id is None:
        return abort(404)

    # # check if name is one of allowable rule types
    specification_pipelines = get_expected_pipeline_specs()
    if ruletype_name not in specification_pipelines.keys():
        return abort(404)

    pipeline = PipelineModel.from_orm(dataset.collection.pipeline).dict(by_alias=True)

    rules = pipeline[ruletype_name]
    if len(rules) > 1000:
        rules = rules[:1000]
        limited = True

    return render_template(
        "dataset/rules.html",
        dataset=dataset,
        ruletype_name=ruletype_name,
        ruletype_specification=specification_pipelines[ruletype_name],
        rules=rules,
        limited=limited,
    )


def get_rule(id, ruletype):
    return PIPELINE_MODELS[ruletype].query.get(id)


@dataset_bp.get("/<string:dataset_id>/rules/<string:ruletype_name>/<rule_id>")
def editrule(dataset_id, ruletype_name, rule_id):
    dataset = Dataset.query.get(dataset_id)

    if dataset is None or dataset.collection_id is None:
        return abort(404)

    specification_pipelines = get_expected_pipeline_specs()
    if ruletype_name not in specification_pipelines.keys():
        return abort(404)

    if rule_id == "new":
        # create empty rule except for dataset
        rule = {"dataset": dataset}
    else:
        rule = get_rule(rule_id, ruletype_name)
        if rule is None:
            return abort(404)

    return render_template(
        "dataset/editrule.html",
        dataset=dataset,
        ruletype_name=ruletype_name,
        ruletype_specification=specification_pipelines[ruletype_name],
        rule=rule,
    )
