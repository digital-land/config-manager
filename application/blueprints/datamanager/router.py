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

from .controllers.form import (
    handle_dashboard_get,
    handle_dashboard_add,
    handle_dashboard_add_import,
    handle_add_data,
)
from .controllers import ControllerError
from .controllers.check import (
    handle_check_results,
    handle_check_resubmit,
)
from .controllers.add import (
    handle_entities_preview,
    handle_add_data_confirm,
)
from .services.async_api import (
    AsyncAPIError,
    fetch_request,
)
from .utils import (
    handle_error,
    inject_now,
)

datamanager_bp = Blueprint("datamanager", __name__, url_prefix="/datamanager")
logger = logging.getLogger(__name__)

datamanager_bp.errorhandler(Exception)(handle_error)
datamanager_bp.context_processor(inject_now)


@datamanager_bp.before_request
def require_login():
    """Require login for all datamanager routes"""
    if current_app.config.get("AUTHENTICATION_ON", True):
        if session.get("user") is None:
            return redirect(url_for("auth.login", next=request.url))


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


def add_data_confirm_async(request_id):
    logger.info(f"Triggering async GitHub workflow for request_id: {request_id}")

    try:
        return handle_add_data_confirm(request_id)
    except ControllerError as e:
        return render_template("datamanager/error.html", message=e.message)


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
    "/add-data/<request_id>/confirm-async",
    view_func=add_data_confirm_async,
    methods=["POST"],
)
