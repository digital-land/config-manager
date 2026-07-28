import json
import logging

from flask import (
    Blueprint,
    current_app,
    redirect,
    render_template,
    request,
    url_for,
    session,
)
from sqlalchemy.exc import SQLAlchemyError
from werkzeug.exceptions import RequestEntityTooLarge

from application.blueprints.base.views import ADD_DATA_LOCK, ASSIGN_ENTITIES_LOCK
from application.db.models import RequestMeta, ServiceLock
from application.extensions import db
from application.utils import compute_hash

from .controllers.form import (
    handle_dashboard_get,
    handle_dashboard_add,
    handle_dashboard_add_import,
    handle_add_data,
)
from .controllers.flagged_resources import (
    REQUIRED_COLUMNS,
    _submit_assign_entities_request,
    handle_flagged_resource_detail,
    handle_flagged_resource_submit,
    handle_flagged_resources_import,
    handle_flagged_resources_start,
    handle_flagged_resources_summary,
)
from .controllers import ControllerError
from .controllers.check import (
    handle_check_results,
    handle_check_resubmit,
)
from .controllers.preview import (
    handle_entities_preview,
    handle_add_data_confirm,
)
from .controllers.transform import handle_check_transform
from .services.duplicates import parse_selected_redirects
from .services.async_api import (
    AsyncAPIError,
    fetch_request,
)
from .utils import (
    handle_error,
    inject_now,
)

# Routes that physically live under datamanager but are also used by the
# assign-entities flow. For these the applicable process lock depends on which
# flow the request belongs to, not on the URL prefix.
_SHARED_FLOW_ENDPOINTS = {
    "datamanager.entities_preview",
    "datamanager.add_data_confirm_async",
}

datamanager_bp = Blueprint("datamanager", __name__, url_prefix="/datamanager")
assign_entities_bp = Blueprint(
    "assign_entities", __name__, url_prefix="/assign-entities"
)
logger = logging.getLogger(__name__)

datamanager_bp.errorhandler(Exception)(handle_error)
datamanager_bp.context_processor(inject_now)
assign_entities_bp.errorhandler(Exception)(handle_error)
assign_entities_bp.context_processor(inject_now)


@assign_entities_bp.errorhandler(RequestEntityTooLarge)
def handle_assign_entities_request_entity_too_large(e):
    if request.endpoint == "assign_entities.flagged_resources_import":
        is_upload = (request.content_type or "").startswith("multipart/form-data")
        message = (
            "The uploaded CSV is too large. Upload a file smaller than 10MB."
            if is_upload
            else "The pasted CSV is too large. Upload the CSV file instead."
        )
        return (
            render_template(
                "datamanager/flagged-resources-import.html",
                csv_data="",
                errors={"csv_data": message},
                required_columns=REQUIRED_COLUMNS,
            ),
            413,
        )

    return render_template("datamanager/error.html", message=str(e)), 413


def _require_login():
    if current_app.config.get("AUTHENTICATION_ON", True):
        if session.get("user") is None:
            return redirect(url_for("auth.login", next=request.url))


def _require_add_data_unlocked():
    try:
        lock = db.session.get(ServiceLock, ADD_DATA_LOCK)
    except SQLAlchemyError:
        return (
            render_template(
                "datamanager/error.html",
                message="The Add Data lock state is unavailable. Try again later.",
            ),
            503,
        )
    if lock:
        return redirect(url_for("base.index", add_data_blocked_by=lock.locked_by))


def _require_assign_entities_unlocked():
    try:
        lock = db.session.get(ServiceLock, ASSIGN_ENTITIES_LOCK)
    except SQLAlchemyError:
        return (
            render_template(
                "datamanager/error.html",
                message=(
                    "The Assign Entities lock state is unavailable. Try again later."
                ),
            ),
            503,
        )
    if lock:
        return redirect(
            url_for("base.index", assign_entities_blocked_by=lock.locked_by)
        )


def _request_is_assign_entities_flow():
    """Whether a shared datamanager route belongs to the assign-entities flow."""
    request_id = (request.view_args or {}).get("request_id")
    if not request_id:
        return False
    try:
        meta = db.session.get(RequestMeta, request_id)
    except SQLAlchemyError:
        return False
    return bool(meta and meta.source_flow == "assign_entities")


@datamanager_bp.before_request
def require_login():
    """Require login for all datamanager routes"""
    login_response = _require_login()
    if login_response:
        return login_response

    # The entities preview is used by both add-data and assign-entities flows
    if (
        request.endpoint in _SHARED_FLOW_ENDPOINTS
        and _request_is_assign_entities_flow()
    ):
        return _require_assign_entities_unlocked()

    return _require_add_data_unlocked()


@assign_entities_bp.before_request
def assign_entities_require_login():
    """Require login for assign entities routes."""
    login_response = _require_login()
    if login_response:
        return login_response

    return _require_assign_entities_unlocked()


# TODO: remove these view functions and move logic entirely into controllers


def dashboard_get():
    return handle_dashboard_get()


def dashboard_add():
    logger.debug("Received form POST data:")
    logger.debug(json.dumps(request.form.to_dict(), indent=2))
    return handle_dashboard_add()


def dashboard_add_import():
    if request.method == "POST":
        logger.debug("Import POST data:")
        logger.debug(json.dumps(request.form.to_dict(), indent=2))
    return handle_dashboard_add_import()


def check_results(request_id):
    """Fetch and display check results from the async API."""
    try:
        result = fetch_request(request_id)
    except AsyncAPIError:
        return (
            render_template(
                "datamanager/error.html",
                message="Error in fetching check results from the Async",
            ),
            404,
        )

    if result.get("status") == "FAILED":
        return (
            render_template(
                "datamanager/error.html",
                message="The check request failed during processing. Please review the request details and try again.",
            ),
            404,
        )

    logger.info(f"Result status: {result.get('status')} for request_id: {request_id}")

    try:
        return handle_check_results(request_id, result)
    except ControllerError as e:
        return render_template("datamanager/error.html", message=e.message)


def check_results_post(request_id):
    """Re-run check with updated pipeline configuration (e.g. column mappings)."""
    try:
        return handle_check_resubmit(request_id)
    except ControllerError as e:
        return render_template("datamanager/error.html", message=e.message)


def add_data(request_id):
    """Entry point for add data form. Submits to async workflow and redirects to entities preview."""
    return handle_add_data(request_id)


def entities_preview(request_id):
    try:
        req = fetch_request(request_id)
    except AsyncAPIError:
        return (
            render_template("datamanager/error.html", message="Preview not found"),
            404,
        )

    logger.info(
        f"Entities preview for request_id: {request_id}, status: {req.get('status')}"
    )

    try:
        return handle_entities_preview(request_id, req)
    except ControllerError as e:
        return render_template("datamanager/error.html", message=e.message)


def check_transform(request_id):
    """Fetch and display transformed facts while the add_data job runs."""
    try:
        req = fetch_request(request_id)
    except AsyncAPIError:
        return (
            render_template(
                "datamanager/error.html", message="Transform request not found"
            ),
            404,
        )

    try:
        return handle_check_transform(request_id, req)
    except ControllerError as e:
        return render_template("datamanager/error.html", message=e.message)


def check_transform_post(request_id):
    """Store selected endpoints to retire/unretire from the transform page.
    """
    checked = request.form.getlist("retire_endpoints")
    presented = request.form.getlist("presented_endpoints")
    currently_retired = request.form.getlist("currently_retired")

    # Never allow the endpoint being added (already in the CSV) to be changed,
    current_hash = _current_endpoint_hash(request_id)
    presented = [h for h in presented if h != current_hash]

    # to_retire = presented and checked and not currently retired → newly retiring
    # to_unretire = currently retired and presented and now unchecked → unretiring
    to_retire = [
        h for h in presented if h in checked and h not in currently_retired
    ]
    to_unretire = [
        h for h in currently_retired if h in presented and h not in checked
    ]

    meta = db.session.get(RequestMeta, request_id)
    if meta is None:
        meta = RequestMeta(
            request_id=request_id,
            endpoints_to_retire=json.dumps(to_retire),
            endpoints_to_unretire=json.dumps(to_unretire),
        )
        db.session.add(meta)
    else:
        meta.endpoints_to_retire = json.dumps(to_retire)
        meta.endpoints_to_unretire = json.dumps(to_unretire)
    db.session.commit()
    return redirect(url_for("datamanager.entities_preview", request_id=request_id))


def _current_endpoint_hash(request_id):
    """sha256 of the endpoint URL being added, or None if it can't be resolved."""
    try:
        req = fetch_request(request_id)
    except AsyncAPIError:
        return None
    url = (req.get("params") or {}).get("url", "")
    return compute_hash(url) if url else None


def add_data_confirm_async(request_id):
    logger.info(f"Triggering async GitHub workflow for request_id: {request_id}")
    github_branch = request.form.get("github_branch") or None
    source_flow = request.form.get("source_flow") or "add_data"
    return_url = request.form.get("return_url") or None

    try:
        return handle_add_data_confirm(
            request_id,
            github_branch=github_branch,
            source_flow=source_flow,
            return_url=return_url,
        )
    except ControllerError as e:
        return render_template("datamanager/error.html", message=e.message)


def flagged_resources_start():
    return handle_flagged_resources_start()


def flagged_resources_import():
    return handle_flagged_resources_import()


def flagged_resources_summary():
    return handle_flagged_resources_summary()


def flagged_resource_submit():
    try:
        return handle_flagged_resource_submit()
    except ControllerError as e:
        return render_template("datamanager/error.html", message=e.message)


def flagged_resource_detail(request_id):
    try:
        return handle_flagged_resource_detail(request_id)
    except AsyncAPIError:
        return (
            render_template(
                "datamanager/error.html",
                message="Assign entities request not found",
            ),
            404,
        )
    except ControllerError as e:
        return render_template("datamanager/error.html", message=e.message)


def _normalise_reference_values(values):
    """Return submitted reference values as a de-duplicated, ordered list."""
    references = []
    seen = set()
    for value in values or []:
        reference = str(value or "").strip()
        if not reference or reference in seen:
            continue
        references.append(reference)
        seen.add(reference)
    return references


def _merge_visible_excluded_references(
    current_excluded_references,
    visible_references,
    selected_visible_references,
):
    """Merge current exclusions with checkbox state from the visible page."""
    current_excluded = set(_normalise_reference_values(current_excluded_references))
    visible = set(_normalise_reference_values(visible_references))
    selected_visible = set(_normalise_reference_values(selected_visible_references))
    excluded = (current_excluded - visible) | (visible - selected_visible)
    return sorted(excluded)


def _selected_redirects_for_async(redirects, excluded_references=None):
    """Map validated Dedup rows to async selected_redirects params.

    Config-manager's Dedup form values use old/new entity field names, while
    async expects ``reference`` and ``old_entity_number``. Redirects for
    explicitly excluded references are dropped because async can only redirect
    entities being assigned.
    """
    excluded_references = set(_normalise_reference_values(excluded_references))
    selected_redirects = []
    seen = set()
    for redirect_row in redirects:
        reference = str(
            redirect_row.get("new_reference") or redirect_row.get("reference") or ""
        ).strip()
        old_entity_number = str(
            redirect_row.get("old_entity")
            or redirect_row.get("old_entity_number")
            or ""
        ).strip()
        if reference in excluded_references:
            continue
        key = (reference, old_entity_number)
        if not all(key) or key in seen:
            continue
        selected_redirects.append(
            {
                "reference": reference,
                "old_entity_number": old_entity_number,
            }
        )
        seen.add(key)
    return selected_redirects


def flagged_resource_detail_post(request_id):
    req = fetch_request(request_id)
    params = req.get("params") or {}
    response_data = (req.get("response") or {}).get("data") or {}
    pipeline_summary = response_data.get("pipeline-summary") or {}
    organisation = params.get("organisation") or params.get("organisationName") or None
    duplicate_candidates = pipeline_summary.get("duplicate-candidates") or []
    redirects = parse_selected_redirects(
        request.form.getlist("entity_redirects"), duplicate_candidates
    )
    if request.form.get("entity_selection_changed") == "true":
        excluded_references = _merge_visible_excluded_references(
            params.get("excluded_references") or [],
            request.form.getlist("visible_entity_references"),
            request.form.getlist("selected_entity_references"),
        )
        try:
            new_request_id = _submit_assign_entities_request(
                params.get("dataset", ""),
                params.get("resource", ""),
                organisation=organisation,
                return_endpoint=params.get("return_endpoint")
                or "assign_entities.flagged_resources_start",
                excluded_references=excluded_references,
                selected_redirects=_selected_redirects_for_async(
                    redirects, excluded_references
                ),
            )
        except AsyncAPIError as e:
            raise ControllerError(
                f"Assign entities submission failed: {e.detail}"
            ) from e
        return redirect(
            url_for(
                "assign_entities.flagged_resource_detail", request_id=new_request_id
            )
        )

    return redirect(url_for("datamanager.entities_preview", request_id=request_id))


datamanager_bp.add_url_rule("/", view_func=dashboard_get, methods=["GET"])
datamanager_bp.add_url_rule("/", view_func=dashboard_add, methods=["POST"])
datamanager_bp.add_url_rule(
    "/import", view_func=dashboard_add_import, methods=["GET", "POST"]
)
datamanager_bp.add_url_rule(
    "/check-results/<request_id>", view_func=check_results, methods=["GET"]
)
datamanager_bp.add_url_rule(
    "/check-results/<request_id>", view_func=check_results_post, methods=["POST"]
)
datamanager_bp.add_url_rule(
    "/add-data/<request_id>", view_func=add_data, methods=["GET", "POST"]
)
datamanager_bp.add_url_rule(
    "/add-data/<request_id>/entities",
    view_func=entities_preview,
    methods=["GET"],
)
datamanager_bp.add_url_rule(
    "/check-transform/<request_id>", view_func=check_transform, methods=["GET"]
)
datamanager_bp.add_url_rule(
    "/check-transform/<request_id>", view_func=check_transform_post, methods=["POST"]
)
datamanager_bp.add_url_rule(
    "/add-data/<request_id>/confirm-async",
    view_func=add_data_confirm_async,
    methods=["POST"],
)
assign_entities_bp.add_url_rule(
    "/",
    view_func=flagged_resources_start,
    methods=["GET", "POST"],
    strict_slashes=False,
)
assign_entities_bp.add_url_rule(
    "/import",
    view_func=flagged_resources_import,
    methods=["GET", "POST"],
)
assign_entities_bp.add_url_rule(
    "/resources",
    view_func=flagged_resources_summary,
    methods=["GET"],
)
assign_entities_bp.add_url_rule(
    "/resource",
    view_func=flagged_resource_submit,
    methods=["POST"],
)
assign_entities_bp.add_url_rule(
    "/check-results/<request_id>",
    view_func=flagged_resource_detail,
    methods=["GET"],
)
assign_entities_bp.add_url_rule(
    "/check-results/<request_id>",
    view_func=flagged_resource_detail_post,
    methods=["POST"],
)
