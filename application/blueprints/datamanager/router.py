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


@datamanager_bp.before_request
def require_login():
    """Require login for all datamanager routes"""
    login_response = _require_login()
    if login_response:
        return login_response

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
    """Store selected endpoints to retire from transform page and continue to preview."""
    hashes = request.form.getlist("retire_endpoints")
    meta = db.session.get(RequestMeta, request_id)
    if meta is None:
        meta = RequestMeta(
            request_id=request_id, endpoints_to_retire=json.dumps(hashes)
        )
        db.session.add(meta)
    else:
        meta.endpoints_to_retire = json.dumps(hashes)
    db.session.commit()
    return redirect(url_for("datamanager.entities_preview", request_id=request_id))


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


def _selected_entity_key(entity):
    """Return the Assign Entities selection identity for an entity row."""
    return (
        str(entity.get("organisation", "")).strip(),
        str(entity.get("reference", "")).strip(),
    )


def _parse_selected_entities(values, new_entities):
    """Validate submitted entity checkbox values against async candidate rows.

    The browser submits JSON values with only organisation and reference. Those
    values are accepted only when the same pair exists in the current async
    response, so hidden/form tampering cannot add extra references.
    """
    valid_entities_by_key = {}
    for entity in new_entities:
        if not isinstance(entity, dict):
            continue
        key = _selected_entity_key(entity)
        if all(key):
            valid_entities_by_key.setdefault(
                key,
                {
                    "organisation": key[0],
                    "reference": key[1],
                },
            )

    selected = []
    seen = set()
    for value in values:
        try:
            submitted = json.loads(value)
        except (TypeError, json.JSONDecodeError):
            continue
        if not isinstance(submitted, dict):
            continue
        key = _selected_entity_key(submitted)
        if key not in valid_entities_by_key or key in seen:
            continue
        selected.append(valid_entities_by_key[key])
        seen.add(key)

    return selected, list(valid_entities_by_key.values())


def _current_selected_entities(current_selected_entities, all_entities):
    """Expand request-param selected_entities into the current candidate rows."""
    if not current_selected_entities:
        return all_entities

    current_keys = {
        _selected_entity_key(entity)
        for entity in current_selected_entities
        if isinstance(entity, dict)
    }
    return [
        entity
        for entity in all_entities
        if _selected_entity_key(entity) in current_keys
    ]


def _merge_visible_selected_entities(
    current_entities, visible_entities, selected_visible
):
    """Merge current full selection with selections from the visible page.

    Entity search and pagination mean the browser only posts rows rendered on
    the current page. This keeps selections from other pages/search states and
    replaces only the visible subset with the user's latest checkbox state.
    """
    visible_keys = {_selected_entity_key(entity) for entity in visible_entities}
    selected_visible_keys = {
        _selected_entity_key(entity) for entity in selected_visible
    }
    selected_keys = {
        _selected_entity_key(entity)
        for entity in current_entities
        if _selected_entity_key(entity) not in visible_keys
    } | selected_visible_keys
    merged = []
    seen = set()
    for entity in current_entities + selected_visible:
        key = _selected_entity_key(entity)
        if key in selected_keys and key not in seen:
            merged.append(entity)
            seen.add(key)
    return merged


def _selected_redirects_for_async(redirects, organisation, selected_entities=None):
    """Map validated Dedup rows to async selected_redirects params.

    Config-manager's Dedup form values use old/new entity field names, while
    async expects ``organisation``, ``reference`` and ``old_entity_number``.
    When selected_entities is supplied, redirects for unselected entities are
    dropped because async can only redirect entities being assigned.
    """
    selected_entity_keys = (
        {_selected_entity_key(entity) for entity in selected_entities}
        if selected_entities is not None
        else None
    )
    selected_redirects = []
    seen = set()
    for redirect_row in redirects:
        reference = str(
            redirect_row.get("new_reference") or redirect_row.get("reference") or ""
        ).strip()
        redirect_organisation = str(
            redirect_row.get("new_organisation")
            or redirect_row.get("organisation")
            or organisation
            or ""
        ).strip()
        old_entity_number = str(
            redirect_row.get("old_entity")
            or redirect_row.get("old_entity_number")
            or ""
        ).strip()
        if (
            selected_entity_keys is not None
            and (redirect_organisation, reference) not in selected_entity_keys
        ):
            continue
        key = (redirect_organisation, reference, old_entity_number)
        if not all(key) or key in seen:
            continue
        selected_redirects.append(
            {
                "organisation": redirect_organisation,
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
    selectable_entities = (
        pipeline_summary.get("all-entities")
        or pipeline_summary.get("new-entities")
        or []
    )
    selected_entities, all_selectable_entities = _parse_selected_entities(
        request.form.getlist("selected_entities"), selectable_entities
    )
    if request.form.get("entity_selection_changed") == "true":
        visible_entities, _ = _parse_selected_entities(
            request.form.getlist("visible_selected_entities"), selectable_entities
        )
        current_entities = _current_selected_entities(
            params.get("selected_entities"), all_selectable_entities
        )
        selected_entities = _merge_visible_selected_entities(
            current_entities, visible_entities, selected_entities
        )
        if not selected_entities:
            raise ControllerError(
                "Select at least one new entity. The async service treats an empty selection as selecting all entities."
            )
        try:
            new_request_id = _submit_assign_entities_request(
                params.get("dataset", ""),
                params.get("resource", ""),
                organisation=organisation,
                return_endpoint=params.get("return_endpoint")
                or "assign_entities.flagged_resources_start",
                selected_entities=selected_entities,
                selected_redirects=_selected_redirects_for_async(
                    redirects, organisation, selected_entities
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

    meta = db.session.get(RequestMeta, request_id)
    if meta is None:
        meta = RequestMeta(
            request_id=request_id,
            entity_redirects=json.dumps(redirects),
        )
        db.session.add(meta)
    else:
        meta.entity_redirects = json.dumps(redirects)
    db.session.commit()
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
