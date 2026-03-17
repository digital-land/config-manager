from datetime import datetime

from flask import Blueprint, current_app, redirect, render_template, request, session, url_for

from application.db.models import ServiceLock
from application.extensions import db

base = Blueprint("base", __name__)

ADD_DATA_LOCK = "add_data"


@base.route("/")
@base.route("/index")
def index():
    authentication_on = current_app.config.get("AUTHENTICATION_ON", True)
    add_data_blocked_by = request.args.get("add_data_blocked_by")
    try:
        add_data_lock = db.session.get(ServiceLock, ADD_DATA_LOCK)
    except Exception:
        add_data_lock = None
    return render_template(
        "index.html",
        authentication_on=authentication_on,
        add_data_lock=add_data_lock,
        add_data_blocked_by=add_data_blocked_by,
    )


@base.route("/process-lock/add-data/toggle", methods=["POST"])
def toggle_add_data_lock():
    user = session.get("user", {})
    username = user.get("login", "unknown")

    lock = db.session.get(ServiceLock, ADD_DATA_LOCK)
    if lock:
        db.session.delete(lock)
    else:
        db.session.add(ServiceLock(name=ADD_DATA_LOCK, locked_by=username, locked_at=datetime.utcnow()))
    db.session.commit()
    return redirect(url_for("base.index"))


@base.route("/health", strict_slashes=False)
def healthz():
    return "OK", 200
