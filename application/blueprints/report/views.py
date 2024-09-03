from urllib.parse import unquote

from flask import Blueprint, abort, redirect, render_template, request, send_file
from flask.helpers import url_for

from application.data_access.endpoint.endpoint_queries import (
    get_endpoint_details,
    get_resources,
)
from application.data_access.odp_summaries.conformance import (
    get_odp_conformance_summary,
)
from application.data_access.odp_summaries.issue import (
    get_odp_issue_summary,
    get_odp_issues_by_issue_type,
)
from application.data_access.odp_summaries.status import get_odp_status_summary
from application.data_access.odp_summaries.utils import generate_odp_summary_csv
from application.data_access.overview.datasette_queries import (
    fetch_resource_from_dataset,
)
from application.data_access.overview.digital_land_queries import (
    fetch_total_resource_count,
    get_content_type_counts,
    get_grouped_source_counts,
    get_log_summary,
    get_organisation_stats,
    get_publisher_coverage,
    get_resource,
    get_resource_count_per_dataset,
    get_source_counts,
    get_sources,
)
from application.data_access.overview.entity_queries import (
    get_entity_count,
    get_grouped_entity_count,
)
from application.data_access.overview.issue_summary import (
    get_issue_summary,
    get_issue_summary_for_csv,
)
from application.data_access.overview.source_and_resource_queries import (
    get_datasets_summary,
    get_monthly_counts,
    get_new_resources,
)
from application.data_access.overview.utils import generate_overview_issue_summary_csv
from application.data_access.summary_queries import (
    get_contributions_and_erroring_endpoints,
    get_contributions_and_errors_by_day,
    get_endpoint_errors_and_successes_by_week,
    get_endpoints_added_by_week,
    get_issue_counts,
)
from application.utils import (
    create_dict,
    filter_off_btns,
    index_by,
    recent_dates,
    yesterday,
)

report_bp = Blueprint("reporting", __name__, url_prefix="/reporting")


@report_bp.get("/")
@report_bp.get("/overview")
def overview():
    contributions_and_errors_by_day_df = get_contributions_and_errors_by_day()
    (
        summary_contributions,
        summary_endpoint_errors,
    ) = get_contributions_and_erroring_endpoints(contributions_and_errors_by_day_df)
    errors, warnings = get_issue_counts()
    endpoints_added_timeseries = get_endpoints_added_by_week()
    (
        endpoint_successes_timeseries,
        endpoint_successes_percentages_timeseries,
        endpoint_errors_percentages_timeseries,
    ) = get_endpoint_errors_and_successes_by_week(contributions_and_errors_by_day_df)
    summary_metrics = {
        "contributions": summary_contributions,
        "endpoint_errors": summary_endpoint_errors,
        "errors": errors,
        "warnings": warnings,
    }
    graphs = {
        "endpoints_added_timeseries": endpoints_added_timeseries,
        "endpoint_successes_timeseries": endpoint_successes_timeseries,
        "endpoint_successes_percentages_timeseries": endpoint_successes_percentages_timeseries,
        "endpoint_errors_percentages_timeseries": endpoint_errors_percentages_timeseries,
    }

    issue_summary = get_issue_summary()

    return render_template(
        "reporting/overview.html",
        summary_metrics=summary_metrics,
        graphs=graphs,
        issue_summary=issue_summary,
    )


@report_bp.get("/odp-summary/status")
def odp_status_summary():
    dataset_types = request.args.getlist("dataset_type")
    cohorts = request.args.getlist("cohort")
    odp_statuses_summary = get_odp_status_summary(dataset_types, cohorts)

    return render_template(
        "reporting/odp_status_summary.html", odp_statuses_summary=odp_statuses_summary
    )


@report_bp.get("/odp-summary/issue")
def odp_issue_summary():
    dataset_types = request.args.getlist("dataset_type")
    cohorts = request.args.getlist("cohort")
    odp_issues_summary = get_odp_issue_summary(dataset_types, cohorts)

    return render_template(
        "reporting/odp_issue_summary.html", odp_issues_summary=odp_issues_summary
    )


@report_bp.get("/odp-summary/conformance")
def odp_conformance_summary():
    dataset_types = request.args.getlist("dataset_type")
    cohorts = request.args.getlist("cohort")
    odp_conformance_summary, conformance_df = get_odp_conformance_summary(
        dataset_types, cohorts
    )
    return render_template(
        "reporting/odp_conformance_summary.html",
        odp_conformance_summary=odp_conformance_summary,
    )


@report_bp.get("/download")
def download_csv():
    type = request.args.get("type")
    dataset_types = request.args.getlist("dataset_type")
    cohorts = request.args.getlist("cohort")
    if type == "odp-status":
        odp_statuses_summary = get_odp_status_summary(dataset_types, cohorts)
        file_path = generate_odp_summary_csv(odp_statuses_summary)
        return send_file(file_path, download_name="odp-status.csv")
    if type == "odp-issue":
        odp_issues_by_type_summary = get_odp_issues_by_issue_type(
            dataset_types, cohorts
        )
        file_path = generate_odp_summary_csv(odp_issues_by_type_summary)
        return send_file(file_path, download_name="odp-issue.csv")
    if type == "odp-conformance":
        odp_conformance_summary, conformance_df = get_odp_conformance_summary(
            dataset_types, cohorts
        )
        file_path = generate_odp_summary_csv(conformance_df)
        return send_file(file_path, download_name="odp-conformance.csv")
    if type == "issue-summary":
        overview_issue_summary = get_issue_summary_for_csv()
        file_path = generate_overview_issue_summary_csv(overview_issue_summary)
        return send_file(file_path, download_name="overview_issue_summary.csv")


@report_bp.get("endpoint/<endpoint_hash>")
def endpoint_details(endpoint_hash):
    endpoint_details = get_endpoint_details(endpoint_hash)
    return render_template(
        "reporting/endpoint_details.html", endpoint_details=endpoint_details
    )


@report_bp.get("/overview-of-datasets")
def overview_of_datasets():
    gs_datasets = get_datasets_summary()
    entity_counts = get_grouped_entity_count()
    content_type_counts = sorted(
        get_content_type_counts(),
        key=lambda x: x["resource_count"],
        reverse=True,
    )

    return render_template(
        "overview/performance.html",
        datasets=gs_datasets,
        stats=get_monthly_counts(),
        publisher_count=get_publisher_coverage(),
        source_counts=get_grouped_source_counts(groupby="dataset"),
        entity_count=get_entity_count(),
        datasets_with_data_count=len(entity_counts.keys()),
        resource_count=fetch_total_resource_count(),
        content_type_counts=content_type_counts,
        new_resources=get_new_resources(dates=recent_dates(7)),
    )


@report_bp.route("/resource")
def resources():
    filters = {}
    if request.args.get("pipeline"):
        filters["pipeline"] = request.args.get("pipeline")
    if request.args.get("content_type"):
        filters["content_type"] = unquote(request.args.get("content_type"))
    if request.args.get("organisation"):
        filters["organisation"] = request.args.get("organisation")
    if request.args.get("resource"):
        filters["resource"] = request.args.get("resource")

    resources_per_dataset = index_by("pipeline", get_resource_count_per_dataset())

    if len(filters.keys()):
        resource_records_results = get_resources(filters=filters)
    else:
        resource_records_results = get_resources()

    content_type_counts = sorted(
        get_content_type_counts(),
        key=lambda x: x["resource_count"],
        reverse=True,
    )

    columns = resource_records_results[0].keys() if resource_records_results else []
    resource_results = [create_dict(columns, row) for row in resource_records_results]

    return render_template(
        "resource/index.html",
        by_dataset=resources_per_dataset,
        resource_count=fetch_total_resource_count(),
        content_type_counts=content_type_counts,
        datasets=get_grouped_entity_count(),
        resources=resource_results,
        filters=filters,
        filter_btns=filter_off_btns(filters),
        organisations=get_organisation_stats(),
    )


@report_bp.route("/logs")
def logs():
    if (
        request.args.get("log-date-day")
        and request.args.get("log-date-month")
        and request.args.get("log-date-year")
    ):
        log_year = request.args.get("log-date-year")
        log_month = request.args.get("log-date-month")
        log_day = request.args.get("log-date-day")
        d = f"{log_year}-{log_month}-{log_day}"
        return redirect(url_for("base.log", date=d))

    summary = get_log_summary()

    return render_template(
        "logs/logs.html",
        summary=summary,
        resources=get_new_resources(),
        yesterday=yesterday(string=True),
        endpoint_count=sum([status["count"] for status in summary]),
    )


@report_bp.route("/content-type")
def content_types():
    pipeline = request.args.get("pipeline")

    content_type_counts = sorted(
        (
            get_content_type_counts(dataset=pipeline)
            if pipeline
            else get_content_type_counts()
        ),
        key=lambda x: x["resource_count"],
        reverse=True,
    )

    if pipeline:
        return render_template(
            "content_type/index.html",
            content_type_counts=content_type_counts,
            pipeline=pipeline,
        )

    return render_template(
        "content_type/index.html", content_type_counts=content_type_counts
    )


def paramify(url):
    # there was a problem if the url to search on included url params
    # this can be avoid if all & are replaced with %26
    url = url.replace("&", "%26")
    # replace spaces (' ' or '%20' ) with %2520 - datasette automatically decoded %20
    url = url.replace(" ", "%2520")
    return url.replace("%20", "%2520")


@report_bp.route("/source")
def sources():
    filters = {}
    if request.args.get("pipeline"):
        filters["pipeline"] = request.args.get("pipeline")
    if request.args.get("organisation") is not None:
        filters["organisation"] = request.args.get("organisation")
    if request.args.get("endpoint_url"):
        filters["endpoint_url"] = paramify(request.args.get("endpoint_url"))
    if request.args.get("endpoint_"):
        filters["endpoint_"] = request.args.get("endpoint_")
    if request.args.get("source"):
        filters["source"] = request.args.get("source")
    if request.args.get("documentation_url") is not None:
        filters["documentation_url"] = request.args.get("documentation_url")
    include_blanks = False
    if request.args.get("include_blanks") is not None:
        include_blanks = request.args.get("include_blanks")

    if len(filters.keys()):
        source_records, query_url = get_sources(
            filter=filters, include_blanks=include_blanks
        )
    else:
        source_records, query_url = get_sources(include_blanks=include_blanks)

    return render_template(
        "source/index.html",
        datasets=get_grouped_source_counts(groupby="dataset"),
        counts=get_source_counts()[0],
        sources=source_records,
        filters=filters,
        filter_btns=filter_off_btns(filters),
        organisations=get_grouped_source_counts(groupby="organisation"),
        query_url=query_url,
        include_blanks=include_blanks,
    )


@report_bp.route("/resource/<resource>")
def resource(resource):
    resource_data = get_resource(resource)
    if not resource_data:
        return abort(404)
    dataset = resource_data[0]["pipeline"].split(";")[0]
    return render_template(
        "resource/resource.html",
        resource=resource_data,
        info_page=url_for("base.resource_info", resource=resource),
        resource_counts=fetch_resource_from_dataset(dataset, resource),
    )


@report_bp.route("/source/<source>")
def source(source):
    source_data, q = get_sources(filter={"source": source})
    if len(source_data) == 0:
        # if no source record return check if blank one exists
        source_data, q = get_sources(filter={"source": source}, include_blanks=True)
    resource_result = get_resources(filters={"source": source})

    columns = resource_result[0].keys() if resource_result else []

    return render_template(
        "source/source.html",
        source=source_data[0],
        resources=[create_dict(columns, row) for row in resource_result],
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
