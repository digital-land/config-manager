import logging

from flask import render_template

from . import ControllerError
from ..services.async_api import fetch_response_details
from ..services.dataset import get_dataset_name
from ..services.organisation import get_organisation_name

logger = logging.getLogger(__name__)

_TRANSFORM_COLS = [
    "entry_number",
    "entity",
    "field",
    "value",
    "start-date",
    "end-date",
    "reference-entity",
]

_ISSUE_COLS = [
    "entry-number",
    "field",
    "issue-type",
    "severity",
    "message",
    "description",
    "value",
    "responsibility",
]


def handle_check_transform(request_id, req):
    """Display transformed facts and issue logs from response-details for a request.

    Shows a loading page while the async job is still running, and the full
    transformed data once it completes. A 'Continue' button leads to entities_preview.
    """
    params = req.get("params") or {}
    organisation_code = params.get("organisationName") or params.get("organisation", "")
    dataset_id = params.get("dataset", "")
    organisation_display = get_organisation_name(organisation_code)
    dataset_display = get_dataset_name(dataset_id, default=dataset_id)

    status = req.get("status")

    if status == "FAILED":
        response_payload = req.get("response") or {}
        response_error = response_payload.get("error")
        raise ControllerError(
            response_error.get("errMsg")
            if response_error
            else "Async job failed with no error information"
        )

    if status in {"PENDING", "PROCESSING", "QUEUED"} or req.get("response") is None:
        return render_template(
            "datamanager/check-transform-loading.html",
            request_id=request_id,
            organisation_display=organisation_display,
            dataset_display=dataset_display,
        )

    resp_details = fetch_response_details(request_id)

    response_payload = req.get("response") or {}
    response_data = response_payload.get("data") or {}
    source_summary = response_data.get("source-summary") or {}
    existing_endpoint = source_summary.get("existing_endpoint_for_organisation_dataset") or ""

    pipeline_summary = response_data.get("pipeline-summary") or {}
    new_count = int(pipeline_summary.get("new-in-resource") or 0)
    existing_count = int(pipeline_summary.get("existing-in-resource") or 0)
    if existing_count > 0:
        growth_pct = round((new_count / existing_count) * 100, 1)
        growth_error = growth_pct > 10
    else:
        growth_pct = None
        growth_error = False
    entity_growth_check = {
        "new_count": new_count,
        "existing_count": existing_count,
        "growth_pct": growth_pct,
        "error": growth_error,
    }

    transform_rows = []
    for item in resp_details:
        tr = item.get("transformed_row") or {}
        row = {
            "entry_number": str(item.get("entry_number", "")),
            "entity": str(tr.get("entity", "")),
            "field": str(tr.get("field", "")),
            "value": str(tr.get("value", "")),
            "start-date": str(tr.get("start-date", "")),
            "end-date": str(tr.get("end-date", "")),
            "reference-entity": str(tr.get("reference-entity", "")),
        }
        transform_rows.append(
            {"columns": {c: {"value": row[c]} for c in _TRANSFORM_COLS}}
        )

    transformed_table = {
        "columns": _TRANSFORM_COLS,
        "fields": _TRANSFORM_COLS,
        "rows": transform_rows,
        "columnNameProcessing": "none",
    }

    issue_rows = []
    for item in resp_details:
        for issue in item.get("issue_logs") or []:
            issue_rows.append(
                {
                    "columns": {
                        col: {"value": str(issue.get(col, ""))}
                        for col in _ISSUE_COLS
                    }
                }
            )

    issue_log_table = {
        "columns": _ISSUE_COLS,
        "fields": _ISSUE_COLS,
        "rows": issue_rows,
        "columnNameProcessing": "none",
    }

    return render_template(
        "datamanager/check-transform.html",
        request_id=request_id,
        organisation_display=organisation_display,
        dataset_display=dataset_display,
        transformed_table=transformed_table,
        issue_log_table=issue_log_table,
        existing_endpoint=existing_endpoint,
        entity_growth_check=entity_growth_check,
    )
