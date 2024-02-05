from flask import Blueprint, render_template

reporting_bp = Blueprint("reporting", __name__, url_prefix="/reporting")


@reporting_bp.route("/")
def overview():
    return render_template("reporting/overview.html")
