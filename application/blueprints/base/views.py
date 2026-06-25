from datetime import datetime

from flask import (
    Blueprint,
    current_app,
    redirect,
    render_template,
    request,
    session,
    url_for,
)

from application.db.models import ServiceLock
from application.extensions import db

base = Blueprint("base", __name__)

ADD_DATA_LOCK = "add-data"
ASSIGN_ENTITIES_LOCK = "assign-entities"
PROCESS_LOCKS = [ADD_DATA_LOCK, ASSIGN_ENTITIES_LOCK]


@base.route("/")
@base.route("/index")
def index():
    authentication_on = current_app.config.get("AUTHENTICATION_ON", True)
    add_data_blocked_by = request.args.get("add_data_blocked_by")
    assign_entities_blocked_by = request.args.get("assign_entities_blocked_by")
    try:
        add_data_lock = db.session.get(ServiceLock, ADD_DATA_LOCK)
    except Exception:
        add_data_lock = None
    try:
        assign_entities_lock = db.session.get(ServiceLock, ASSIGN_ENTITIES_LOCK)
    except Exception:
        assign_entities_lock = None
    return render_template(
        "index.html",
        authentication_on=authentication_on,
        add_data_lock=add_data_lock,
        add_data_blocked_by=add_data_blocked_by,
        assign_entities_lock=assign_entities_lock,
        assign_entities_blocked_by=assign_entities_blocked_by,
    )


@base.route("/process-lock/<process>/toggle", methods=["POST"])
def toggle_process_lock(process):
    if process not in PROCESS_LOCKS:
        return redirect(url_for("base.index"))

    user = session.get("user", {})
    if current_app.config.get("AUTHENTICATION_ON", True):
        if not user:
            return redirect(url_for("auth.login", next=request.url))
        username = user["login"]
    else:
        username = user.get("login", "development")

    lock = db.session.get(ServiceLock, process)
    if lock:
        db.session.delete(lock)
    else:
        db.session.add(
            ServiceLock(
                name=process,
                locked_by=username,
                locked_at=datetime.utcnow(),
            )
        )
    db.session.commit()
    return redirect(url_for("base.index"))


@base.route("/health", strict_slashes=False)
def healthz():
    return "OK", 200
