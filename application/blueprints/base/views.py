from flask import Blueprint, current_app, render_template

base = Blueprint("base", __name__)


@base.route("/")
@base.route("/index")
def index():
    authentication_on = current_app.config.get("AUTHENTICATION_ON", True)
    return render_template("index.html", authentication_on=authentication_on)


@base.route("/health", strict_slashes=False)
def healthz():
    return "OK", 200
