import logging

from flask import (
    render_template,
    session,
)

from . import ControllerError
from ..services.github import (
    trigger_add_data_async_workflow,
    GitHubWorkflowError,
)
from ..utils.csv_formats import (
    build_column_csv_preview,
    build_endpoint_csv_preview,
    build_entity_organisation_csv,
    build_lookup_csv_preview,
    build_source_csv_preview,
)

logger = logging.getLogger(__name__)


def handle_entities_preview(request_id, req):
    # Loading state
    if (
        req.get("status") in {"PENDING", "PROCESSING", "QUEUED"}
        or req.get("response") is None
    ):
        return render_template(
            "datamanager/add-data-preview-loading.html", request_id=request_id
        )

    # Error state in async
    response_payload = req.get("response") or {}
    response_error = response_payload.get("error")
    if response_error:
        raise ControllerError(response_error.get("errMsg") or "Unknown error")

    data = response_payload.get("data") or {}

    pipeline_summary = data.get("pipeline-summary") or {}
    endpoint_summary = data.get("endpoint-summary") or {}
    source_summary_data = data.get("source-summary") or {}

    existing_entities_list = pipeline_summary.get("existing-entities") or []
    new_entities = pipeline_summary.get("new-entities") or []
    pipeline_issues = pipeline_summary.get("pipeline-issues") or []

    # Build lookup CSV preview
    table_params, lookup_csv_text = build_lookup_csv_preview(new_entities)

    # Existing entities table
    ex_cols = ["reference", "entity"]
    ex_rows = [
        {"columns": {c: {"value": (e.get(c) or "")} for c in ex_cols}}
        for e in existing_entities_list
    ]
    existing_table_params = {
        "columns": ex_cols,
        "fields": ex_cols,
        "rows": ex_rows,
        "columnNameProcessing": "none",
    }

    # Build endpoint CSV preview
    (
        endpoint_already_exists,
        endpoint_url,
        endpoint_csv_table_params,
        endpoint_csv_text,
    ) = build_endpoint_csv_preview(endpoint_summary)

    # Build source CSV preview
    source_summary, source_csv_table_params, source_csv_text = build_source_csv_preview(
        source_summary_data
    )

    # Build column CSV preview
    params = req.get("params", {}) or {}
    dataset_id = params.get("dataset", "")
    column_mapping = params.get("column_mapping", {})
    (
        column_csv_table_params,
        column_csv_text,
        has_column_mapping,
    ) = build_column_csv_preview(column_mapping, dataset_id, endpoint_summary)

    # Build entity-organisation CSV preview (only for authoritative data)
    authoritative = params.get("authoritative", False)
    entity_org_table_params = None
    entity_org_csv_text = ""
    has_entity_org = False
    entity_org_warning = None

    if authoritative:
        entity_organisation_data = pipeline_summary.get("entity-organisation") or []
        if entity_organisation_data:
            (
                entity_org_table_params,
                entity_org_csv_text,
                has_entity_org,
            ) = build_entity_organisation_csv(entity_organisation_data)
        else:
            entity_org_warning = "No entity-organisation data found"
    else:
        entity_org_warning = (
            "This must be manually created currently for non-authoritative data"
        )

    return render_template(
        "datamanager/entities_preview.html",
        request_id=request_id,
        new_count=int(pipeline_summary.get("new-in-resource") or 0),
        existing_count=int(pipeline_summary.get("existing-in-resource") or 0),
        endpoint_already_exists=endpoint_already_exists,
        endpoint_url=endpoint_url,
        table_params=table_params,
        lookup_csv_text=lookup_csv_text,
        existing_table_params=existing_table_params,
        endpoint_csv_table_params=endpoint_csv_table_params,
        endpoint_csv_text=endpoint_csv_text,
        source_csv_table_params=source_csv_table_params,
        source_csv_text=source_csv_text,
        source_summary=source_summary,
        column_csv_table_params=column_csv_table_params,
        column_csv_text=column_csv_text,
        has_column_mapping=has_column_mapping,
        pipeline_issues=pipeline_issues,
        entity_org_table_params=entity_org_table_params,
        entity_org_csv_text=entity_org_csv_text,
        has_entity_org=has_entity_org,
        entity_org_warning=entity_org_warning,
    )


def handle_add_data_confirm(request_id):
    try:
        result = trigger_add_data_async_workflow(
            request_id=request_id,
            triggered_by=f"config-manager-user-{session.get('user', {}).get('login', 'unknown')}",
        )
    except GitHubWorkflowError as e:
        logger.exception(f"GitHub async workflow error: {e}")
        raise ControllerError(f"GitHub workflow error: {str(e)}") from e

    if not result["success"]:
        logger.error(f"Failed to trigger async workflow: {result['message']}")
        raise ControllerError(f"Failed to trigger async workflow: {result['message']}")

    logger.info(f"Successfully triggered async workflow for request_id: {request_id}")
    return render_template(
        "datamanager/add-data-success.html",
        message=result["message"],
    )
