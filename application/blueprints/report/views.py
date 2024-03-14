from flask import Blueprint, render_template, request

from application.data_access.odp_queries import get_odp_status_summary
from application.data_access.summary_queries import (
    get_contributions_and_erroring_endpoints,
    get_endpoint_errors_and_successes_by_week,
    get_endpoints_added_by_week,
    get_issue_counts,
    get_logs,
)

report_bp = Blueprint("reporting", __name__, url_prefix="/reporting")


@report_bp.get("/")
@report_bp.get("/overview")
def overview():
    # get tabbed elements
    # odp_endpoint_summary = reporting_summary.groupby('dataset')  .....
    # odp_active_resource_summary = ...
    # dataset_summary = ....
    # organisation_summary =  ...

    # content_type_counts = sorted(
    #     get_content_type_counts(),
    #     key=lambda x: x["resource_count"],
    #     reverse=True,
    # )
    logs_df = get_logs()
    (
        summary_contributions,
        summary_endpoint_errors,
    ) = get_contributions_and_erroring_endpoints()
    errors, warnings = get_issue_counts()
    endpoints_added_timeseries = get_endpoints_added_by_week()
    (
        endpoint_successes_timeseries,
        endpoint_errors_timeseries,
    ) = get_endpoint_errors_and_successes_by_week(logs_df)

    summary_metrics = {
        "contributions": summary_contributions,
        "endpoint_errors": summary_endpoint_errors,
        "errors": errors,
        "warnings": warnings,
    }
    graphs = {
        "endpoints_added_timeseries": endpoints_added_timeseries,
        "endpoint_successes_timeseries": endpoint_successes_timeseries,
        "endpoint_errors_timeseries": endpoint_errors_timeseries,
    }

    return render_template(
        "reporting/overview.html", summary_metrics=summary_metrics, graphs=graphs
    )


@report_bp.get("/odp-summary")
def odp_summary():
    dataset_type = request.args.get("dataset_type")
    cohort = request.args.get("cohort")
    odp_statuses_summary = get_odp_status_summary(dataset_type, cohort)
    return render_template(
        "reporting/odp_summary.html", odp_statuses_summary=odp_statuses_summary
    )


# @report_bp.get("/dataset")
# def datasets(dataset_id):
#     dataset = Dataset.query.get(dataset_id)

#     if dataset is None or dataset.collection_id is None:
#         return abort(404)

#     specification_pipelines = get_expected_pipeline_specs()

#     rule_counts = count_pipeline_rules(dataset.collection.pipeline)

#     return render_template(
#         "dataset/dataset.html",
#         pipeline=dataset.collection.pipeline,
#         dataset=dataset,
#         specification_pipelines=specification_pipelines,
#         rule_counts=rule_counts,
#     )


# @report_bp.get("dataset/<string:dataset_id>")
# def dataset(dataset_id, rule_type_name):
#     limited = False
#     dataset = Dataset.query.get(dataset_id)

#     if dataset is None or dataset.collection_id is None:
#         return abort(404)

#     # # check if name is one of allowable rule types
#     specification_pipelines = get_expected_pipeline_specs()
#     if rule_type_name not in specification_pipelines.keys():
#         return abort(404)

#     rules = getattr(dataset.collection.pipeline, rule_type_name)

#     if len(rules) > 1000:
#         rules = rules[:1000]
#         limited = True

#     return render_template(
#         "dataset/rules.html",
#         dataset=dataset,
#         rule_type_name=rule_type_name,
#         rule_type_specification=specification_pipelines[rule_type_name],
#         rules=rules,
#         limited=limited,
#     )


# @report_bp.get("/organisation")
# def organisations(dataset_id):
#     dataset = Dataset.query.get(dataset_id)

#     if dataset is None or dataset.collection_id is None:
#         return abort(404)

#     return render_template(
#         "dataset/sources.html",
#         dataset=dataset,
#     )


# @report_bp.get("/organisation/<string:organisation_id>")
# def organisation(dataset_id, rule_type_name, rule_id):
#     dataset = Dataset.query.get(dataset_id)

#     if dataset is None or dataset.collection_id is None:
#         return abort(404)

#     specification_pipelines = get_expected_pipeline_specs()
#     if rule_type_name not in specification_pipelines.keys():
#         return abort(404)

#     if rule_id == "new":
#         # create empty rule except for dataset

#         form = EditRuleForm(dataset_id=dataset.dataset)
#         form.field_id.choices = [(field.field, field.field) for field in dataset.fields]
#         rule = {"dataset": dataset.dataset}
#     else:
#         rule = get_rule(rule_id, rule_type_name)
#         if rule is None:
#             return abort(404)

#         form = EditRuleForm(obj=rule, rule_type=rule_type_name)
#         if hasattr(form, "field_id"):
#             form.field_id.choices = [
#                 (field.field, field.field) for field in dataset.fields
#             ]
#             if rule.field:
#                 form.field_id.data = rule.field.field

#     rule_type_specification = specification_pipelines[rule_type_name]
#     form_field_names = _get_form_field_names(rule_type_specification)

#     return render_template(
#         "dataset/editrule.html",
#         form=form,
#         dataset=dataset,
#         rule_type_name=rule_type_name,
#         rule_type_specification=rule_type_specification,
#         form_field_names=form_field_names,
#         rule=rule,
#     )
