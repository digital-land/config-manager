import json
import logging

from flask import (
    render_template,
    session,
    url_for,
)

from application.db.models import RequestMeta
from application.extensions import db

from . import ControllerError
from ..services.async_api import fetch_request
from ..services.github import (
    config_branch_changed_for_collection,
    trigger_add_data_async_workflow,
    wait_for_add_data_workflow_idle,
    GitHubWorkflowError,
)
from ..services.dataset import get_dataset_name
from ..services.organisation import get_organisation_name
from ..utils.csv_formats import (
    build_column_csv_preview,
    build_endpoint_csv_preview,
    build_entity_organisation_csv,
    build_lookup_csv_preview,
    build_source_csv_preview,
)

logger = logging.getLogger(__name__)


def _build_entity_organisation_summary(new_entities, authoritative, pipeline_summary):
    """
    Build entity-organisation CSV preview context - only relevant when new
    entities were actually created; otherwise there is nothing to map.

    Returns (entity_org_table_params, has_entity_org, entity_org_warning,
    entity_org_overlap_info, entity_org_error_warning)
    """
    entity_org_table_params = None
    has_entity_org = False
    entity_org_warning = None
    entity_org_overlap_info = None
    entity_org_error_warning = None

    if not new_entities:
        return (
            entity_org_table_params,
            has_entity_org,
            entity_org_warning,
            entity_org_overlap_info,
            entity_org_error_warning,
        )

    if not authoritative:
        entity_org_warning = "Non-authoritative data being submitted"
        return (
            entity_org_table_params,
            has_entity_org,
            entity_org_warning,
            entity_org_overlap_info,
            entity_org_error_warning,
        )

    entity_organisation_data = pipeline_summary.get("entity-organisation") or []
    if entity_organisation_data:
        entry = entity_organisation_data[0]
        if entry.get("overlap"):
            entity_org_overlap_info = "Entity org already exists - no action needed"
        elif entry.get("error"):
            entity_org_error_warning = (
                "An error occurred creating the entity-organisation csv, "
                "please re-run if you believe this is required"
            )
        else:
            (
                entity_org_table_params,
                has_entity_org,
            ) = build_entity_organisation_csv(entity_organisation_data)

    return (
        entity_org_table_params,
        has_entity_org,
        entity_org_warning,
        entity_org_overlap_info,
        entity_org_error_warning,
    )


def _load_json_list(value: str | None) -> list:
    if not value:
        return []
    try:
        loaded = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return []
    return loaded if isinstance(loaded, list) else []


def _build_endpoint_summary(
    selected_hashes, existing_endpoints, dataset_id, organisation_code
):
    summary = []
    if not selected_hashes:
        return summary
    dataset_display = get_dataset_name(dataset_id, default=dataset_id)
    org_display = get_organisation_name(organisation_code)
    for ep in existing_endpoints:
        ep_hash = ep.get("endpoint") if isinstance(ep, dict) else ep
        ep_url = ep.get("endpoint-url", ep_hash) if isinstance(ep, dict) else ep
        if ep_hash in selected_hashes:
            summary.append(
                {
                    "endpoint": ep_hash,
                    "endpoint-url": ep_url,
                    "dataset": dataset_display,
                    "organisation": org_display,
                }
            )
    return summary


def _count_excluded_references(params: dict) -> int:
    references = params.get("excluded_references") or []
    if not isinstance(references, list):
        return 0
    return len(
        {str(reference).strip() for reference in references if str(reference).strip()}
    )


def build_old_entity_redirect_table(old_entity_rows: list[dict]) -> dict | None:
    if not old_entity_rows:
        return None

    columns = [
        "old-entity",
        "status",
        "entity",
        "notes",
        "end-date",
        "entry-date",
        "start-date",
    ]
    rows = []
    for old_entity in old_entity_rows:
        if not isinstance(old_entity, dict):
            continue
        row = {
            "old-entity": str(
                old_entity.get("old-entity", "") or old_entity.get("old_entity", "")
            ),
            "status": str(old_entity.get("status", "") or ""),
            "entity": str(old_entity.get("entity", "") or ""),
            "notes": str(old_entity.get("notes", "") or ""),
            "end-date": str(
                old_entity.get("end-date", "") or old_entity.get("end_date", "")
            ),
            "entry-date": str(
                old_entity.get("entry-date", "") or old_entity.get("entry_date", "")
            ),
            "start-date": str(
                old_entity.get("start-date", "") or old_entity.get("start_date", "")
            ),
        }
        rows.append({"columns": {c: {"value": row[c]} for c in columns}})
    if not rows:
        return None

    return {
        "columns": columns,
        "fields": columns,
        "rows": rows,
        "columnNameProcessing": "none",
    }


def handle_entities_preview(request_id, req):
    # Check State
    status = req.get("status")

    if status == "FAILED":
        response_payload = req.get("response") or {}
        response_error = response_payload.get("error")
        raise ControllerError(
            response_error.get("errMsg")
            if response_error
            else "Async Failed processing for this task with no error information"
        )

    if status in {"PENDING", "PROCESSING", "QUEUED"} or req.get("response") is None:
        return render_template(
            "datamanager/add-data-preview-loading.html", request_id=request_id
        )

    response_payload = req.get("response") or {}
    data = response_payload.get("data") or {}

    pipeline_summary = data.get("pipeline-summary") or {}
    endpoint_summary = data.get("endpoint-summary") or {}
    source_summary_data = data.get("source-summary") or {}

    existing_entities_list = pipeline_summary.get("existing-entities") or []
    new_entities = pipeline_summary.get("new-entities") or []

    # Build lookup CSV preview
    table_params = build_lookup_csv_preview(new_entities)

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
    params = req.get("params", {}) or {}
    endpoint_parameters = params.get("endpoint_parameters") or None
    (
        endpoint_already_exists,
        endpoint_url,
        endpoint_csv_table_params,
    ) = build_endpoint_csv_preview(
        endpoint_summary, endpoint_parameters=endpoint_parameters
    )

    # Build source CSV preview
    source_summary, source_csv_table_params = build_source_csv_preview(
        source_summary_data
    )

    # Build column CSV preview
    dataset_id = params.get("dataset", "")
    column_mapping = params.get("column_mapping", {})
    (
        column_csv_table_params,
        has_column_mapping,
    ) = build_column_csv_preview(column_mapping, dataset_id, endpoint_summary)

    github_branch = params.get("github_branch") or None
    request_meta = db.session.get(RequestMeta, request_id)
    source_flow = (request_meta.source_flow if request_meta else None) or "add_data"
    return_endpoint = params.get("return_endpoint")
    if return_endpoint:
        return_url = url_for(return_endpoint)
    elif source_flow == "assign_entities":
        return_url = url_for("assign_entities.flagged_resources_start")
    else:
        return_url = url_for("datamanager.dashboard_get")

    # Retire endpoint details
    endpoints_to_retire = (
        _load_json_list(request_meta.endpoints_to_retire) if request_meta else []
    )
    endpoints_to_unretire = (
        _load_json_list(request_meta.endpoints_to_unretire) if request_meta else []
    )
    old_entity_rows = pipeline_summary.get("old-entity") or []
    old_entity_redirect_table_params = build_old_entity_redirect_table(old_entity_rows)
    old_entity_redirect_count = (
        len(old_entity_redirect_table_params["rows"])
        if old_entity_redirect_table_params
        else 0
    )
    existing_endpoints = (
        source_summary_data.get("existing_endpoint_for_organisation_dataset") or []
    )
    if isinstance(existing_endpoints, str):
        existing_endpoints = [existing_endpoints] if existing_endpoints else []
    organisation_code = params.get("organisationName") or params.get("organisation", "")

    retire_summary = _build_endpoint_summary(
        endpoints_to_retire, existing_endpoints, dataset_id, organisation_code
    )
    unretire_summary = _build_endpoint_summary(
        endpoints_to_unretire, existing_endpoints, dataset_id, organisation_code
    )

    # Build entity-organisation CSV preview
    authoritative = params.get("authoritative", False)
    (
        entity_org_table_params,
        has_entity_org,
        entity_org_warning,
        entity_org_overlap_info,
        entity_org_error_warning,
    ) = _build_entity_organisation_summary(
        new_entities, authoritative, pipeline_summary
    )

    return render_template(
        "datamanager/entities_preview.html",
        request_id=request_id,
        github_branch=github_branch,
        source_flow=source_flow,
        return_url=return_url,
        retire_summary=retire_summary,
        unretire_summary=unretire_summary,
        old_entity_redirect_table_params=old_entity_redirect_table_params,
        old_entity_redirect_count=old_entity_redirect_count,
        new_count=len(new_entities),
        excluded_count=_count_excluded_references(params),
        existing_count=int(pipeline_summary.get("existing-in-resource") or 0),
        endpoint_already_exists=endpoint_already_exists,
        endpoint_url=endpoint_url,
        table_params=table_params,
        existing_table_params=existing_table_params,
        endpoint_csv_table_params=endpoint_csv_table_params,
        source_csv_table_params=source_csv_table_params,
        source_summary=source_summary,
        column_csv_table_params=column_csv_table_params,
        has_column_mapping=has_column_mapping,
        entity_org_table_params=entity_org_table_params,
        has_entity_org=has_entity_org,
        entity_org_warning=entity_org_warning,
        entity_org_overlap_info=entity_org_overlap_info,
        entity_org_error_warning=entity_org_error_warning,
    )


def handle_add_data_confirm(
    request_id,
    github_branch: str | None = None,
    source_flow: str = "add_data",
    return_url: str | None = None,
):
    request_meta = db.session.get(RequestMeta, request_id)
    endpoints_to_retire = (
        _load_json_list(request_meta.endpoints_to_retire) if request_meta else []
    )
    endpoints_to_unretire = (
        _load_json_list(request_meta.endpoints_to_unretire) if request_meta else []
    )

    # Stale-assessment guard: if the config branch has advanced for this collection
    # since the assessment was taken, the assigned entity numbers may now collide.
    baseline_sha = request_meta.branch_sha if request_meta else None
    if github_branch and baseline_sha:
        # Wait for any in-flight add-data workflow to finish so the compare reads a
        # settled branch (not a mid-push state). Bounded; on timeout we proceed and
        # the fail-closed compare below is the backstop.
        wait_for_add_data_workflow_idle()
        req = fetch_request(request_id)
        collection = (req.get("params") or {}).get("collection")
        if collection and config_branch_changed_for_collection(
            baseline_sha, github_branch, collection
        ):
            logger.info(
                "Blocking stale confirm for request %s: %s advanced for collection %s",
                request_id,
                github_branch,
                collection,
            )
            # Prefer sending the user back to the check-results page they started
            check_request_id = request_meta.check_request_id if request_meta else None
            if check_request_id:
                rerun_url = url_for(
                    "datamanager.check_results", request_id=check_request_id
                )
            else:
                rerun_url = return_url or (
                    url_for("assign_entities.flagged_resources_start")
                    if source_flow == "assign_entities"
                    else url_for("datamanager.dashboard_get")
                )
            return render_template(
                "datamanager/add-data-stale.html",
                collection=collection,
                github_branch=github_branch,
                source_flow=source_flow,
                return_url=rerun_url,
            )

    try:
        result = trigger_add_data_async_workflow(
            request_id=request_id,
            triggered_by=f"{session.get('user', {}).get('login', 'unknown')}",
            github_branch=github_branch,
            endpoints_to_retire=endpoints_to_retire,
            endpoints_to_unretire=endpoints_to_unretire,
        )
    except GitHubWorkflowError as e:
        logger.exception(f"GitHub async workflow error: {e}")
        raise ControllerError(f"GitHub workflow error: {str(e)}") from e

    if not result["success"]:
        logger.error(f"Failed to trigger async workflow: {result['message']}")
        raise ControllerError(f"Failed to trigger async workflow: {result['message']}")

    fallback_return_url = (
        url_for("assign_entities.flagged_resources_start")
        if source_flow == "assign_entities"
        else url_for("datamanager.dashboard_get")
    )
    logger.info(f"Successfully triggered async workflow for request_id: {request_id}")
    return render_template(
        "datamanager/add-data-success.html",
        message=result["message"],
        github_branch=github_branch,
        source_flow=source_flow,
        return_url=return_url or fallback_return_url,
    )
