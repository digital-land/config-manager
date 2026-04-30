import logging
from collections import defaultdict

from flask import render_template, request as flask_request

from . import ControllerError
from ..services.async_api import fetch_response_details
from ..services.dataset import get_dataset_name
from ..services.organisation import get_org_entity, get_organisation_name
from ..services.endpoint import get_endpoint_urls_for_hashes
from ..services.planning_data import get_entities_for_organisation_and_dataset

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

_ENTITY_COL_EXCLUDE = {"geometry", "point", "typology", "prefix"}
_ENTITY_COL_PRIORITY = ["entity", "reference", "name"]
_ENTITY_ROWS_PER_PAGE = 5000


def _normalise_entity_id(raw) -> str:
    if raw is None or raw == "":
        return ""
    try:
        return str(int(float(str(raw))))
    except (ValueError, TypeError):
        return str(raw)


def build_entities_data(resp_details: list, platform_entities: list) -> dict:
    """
    Pivot transformed facts from resp_details by entity and combine with
    platform entities. Returns a dict with 'columns' and 'rows', where each
    row has 'fields' (dict) and 'is_new' (bool).
    """
    pivoted = defaultdict(dict)
    for item in resp_details:
        tr = item.get("transformed_row") or {}
        entity_id = _normalise_entity_id(tr.get("entity", ""))
        field = tr.get("field", "")
        value = tr.get("value", "")
        if entity_id and field:
            pivoted[entity_id][field] = value

    platform_entity_ids = {
        _normalise_entity_id(e.get("entity", "")) for e in platform_entities
    }
    new_entity_ids = set(pivoted.keys()) - platform_entity_ids
    in_both_ids = set(pivoted.keys()) & platform_entity_ids

    all_col_keys = set(_ENTITY_COL_PRIORITY)
    for fields in pivoted.values():
        all_col_keys.update(fields.keys())
    for e in platform_entities:
        all_col_keys.update(e.keys())
    all_col_keys -= _ENTITY_COL_EXCLUDE
    columns = _ENTITY_COL_PRIORITY + sorted(all_col_keys - set(_ENTITY_COL_PRIORITY))

    rows = []
    for entity_id, fields in pivoted.items():
        rows.append(
            {
                "fields": {
                    col: (entity_id if col == "entity" else str(fields.get(col, "")))
                    for col in columns
                },
                "is_new": entity_id in new_entity_ids,
                "is_in_both": entity_id in in_both_ids,
            }
        )
    for e in platform_entities:
        if str(e.get("entity", "")) not in pivoted:
            rows.append(
                {
                    "fields": {col: str(e.get(col, "")) for col in columns},
                    "is_new": False,
                    "is_in_both": False,
                }
            )

    return {"columns": columns, "rows": rows}


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

    entity_page = max(1, int(flask_request.args.get("entity_page", 1)))
    start_offset = (entity_page - 1) * _ENTITY_ROWS_PER_PAGE
    resp_details = fetch_response_details(
        request_id, start_offset=start_offset, max_rows=_ENTITY_ROWS_PER_PAGE
    )
    has_next_entity_page = len(resp_details) >= _ENTITY_ROWS_PER_PAGE

    response_payload = req.get("response") or {}
    response_data = response_payload.get("data") or {}
    source_summary = response_data.get("source-summary") or {}
    existing_endpoints = (
        source_summary.get("existing_endpoint_for_organisation_dataset") or []
    )
    if isinstance(existing_endpoints, str):
        existing_endpoints = [existing_endpoints] if existing_endpoints else []
    if existing_endpoints:
        endpoint_urls = get_endpoint_urls_for_hashes(existing_endpoints)
        existing_endpoints = [
            {"endpoint": h, "endpoint-url": endpoint_urls.get(h, "")}
            for h in existing_endpoints
        ]

    pipeline_summary = response_data.get("pipeline-summary") or {}
    new_count = int(pipeline_summary.get("new-in-resource") or 0)

    # Query Planning Data to get count of existing entities for this org/dataset,
    # to check growth percentage against new count

    org_entity = get_org_entity(organisation_code)
    platform_entities = (
        get_entities_for_organisation_and_dataset(org_entity, dataset_id)
        if org_entity is not None
        else []
    )
    existing_count = len(platform_entities)

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

    entities_data = build_entities_data(resp_details, platform_entities)

    # Create tables for transformed data and issue logs, with empty string defaults
    # to avoid rendering issues with None values. The tables expect all columns to
    # be present in every row, so we ensure that with the dict comprehensions below.

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
            cols = {}
            for col in _ISSUE_COLS:
                val = str(issue.get(col, ""))
                if col == "severity" and val.lower() == "error":
                    cols[col] = {
                        "value": val,
                        "html": (
                            '<span style="background-color:#d4351c;color:white;'
                            'padding:2px 8px;border-radius:3px;">error</span>'
                        ),
                    }
                else:
                    cols[col] = {"value": val}
            issue_rows.append({"columns": cols})

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
        existing_endpoints=existing_endpoints,
        entity_growth_check=entity_growth_check,
        entities_data=entities_data,
        entity_page=entity_page,
        has_next_entity_page=has_next_entity_page,
    )
