import json
import logging
from datetime import date

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
    get_config_baseline_sha,
    trigger_add_data_async_workflow,
    wait_for_add_data_workflow_idle,
    GitHubAppError,
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


def build_old_entity_redirect_table(entity_redirects: list[dict]) -> dict | None:
    if not entity_redirects:
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
    entry_date = date.today().isoformat()
    rows = []
    for redirect in entity_redirects:
        row = {
            "old-entity": str(redirect.get("old_entity", "")),
            "status": "301",
            "entity": str(redirect.get("entity", "")),
            "notes": str(
                redirect.get("notes")
                or "Redirect duplicate entity selected in Assign Entities"
            ),
            "end-date": "",
            "entry-date": entry_date,
            "start-date": "",
        }
        rows.append({"columns": {c: {"value": row[c]} for c in columns}})

    return {
        "columns": columns,
        "fields": columns,
        "rows": rows,
        "columnNameProcessing": "none",
    }


def record_branch_baseline(request_id, github_branch, check_request_id=None):
    """
    Capture the config branch HEAD at assessment-submission time so that, when the
    user later confirms, we can detect whether the branch advanced underneath the
    assessment (which would make the assigned entity numbers stale).
    """
    if not github_branch:
        return
    try:
        # If an add-data workflow is mid-push to the branch, wait for it to settle so
        # the baseline reflects the state the async worker will actually read. Bounded;
        # on timeout we proceed anyway and rely on the confirm-time check.
        wait_for_add_data_workflow_idle()
        sha = get_config_baseline_sha(github_branch)
    except GitHubAppError as e:
        logger.warning("Could not capture branch baseline for %s: %s", request_id, e)
        return
    if not sha:
        return

    meta = db.session.get(RequestMeta, request_id)
    if meta is None:
        meta = RequestMeta(
            request_id=request_id,
            branch_sha=sha,
            check_request_id=check_request_id,
        )
        db.session.add(meta)
    else:
        meta.branch_sha = sha
        if check_request_id:
            meta.check_request_id = check_request_id
    db.session.commit()


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
    source_flow = (
        "assign_entities"
        if params.get("resource") and not params.get("url")
        else "add_data"
    )
    return_endpoint = params.get("return_endpoint")
    if return_endpoint:
        return_url = url_for(return_endpoint)
    elif source_flow == "assign_entities":
        return_url = url_for("assign_entities.flagged_resources_start")
    else:
        return_url = url_for("datamanager.dashboard_get")

    # Retire endpoint details
    request_meta = db.session.get(RequestMeta, request_id)
    endpoints_to_retire = (
        _load_json_list(request_meta.endpoints_to_retire) if request_meta else []
    )
    entity_redirects = (
        _load_json_list(request_meta.entity_redirects) if request_meta else []
    )
    old_entity_redirect_table_params = build_old_entity_redirect_table(entity_redirects)
    existing_endpoints = (
        source_summary_data.get("existing_endpoint_for_organisation_dataset") or []
    )
    if isinstance(existing_endpoints, str):
        existing_endpoints = [existing_endpoints] if existing_endpoints else []
    organisation_code = params.get("organisationName") or params.get("organisation", "")
    retire_summary = []
    if endpoints_to_retire:
        dataset_display = get_dataset_name(dataset_id, default=dataset_id)
        org_display = get_organisation_name(organisation_code)
        for ep in existing_endpoints:
            ep_hash = ep.get("endpoint") if isinstance(ep, dict) else ep
            ep_url = ep.get("endpoint-url", ep_hash) if isinstance(ep, dict) else ep
            if ep_hash in endpoints_to_retire:
                retire_summary.append(
                    {
                        "endpoint": ep_hash,
                        "endpoint-url": ep_url,
                        "dataset": dataset_display,
                        "organisation": org_display,
                    }
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
        entity_redirects=entity_redirects,
        old_entity_redirect_table_params=old_entity_redirect_table_params,
        new_count=int(pipeline_summary.get("new-in-resource") or 0),
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
    entity_redirects = (
        _load_json_list(request_meta.entity_redirects) if request_meta else []
    )

    # Stale-assessment guard: if the config branch has advanced for this collection
    # since the assessment was taken, the assigned entity numbers may now collide.
    baseline_sha = request_meta.branch_sha if request_meta else None
    if github_branch and baseline_sha:
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
            entity_redirects=entity_redirects,
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
