import datetime

from flask import Blueprint, abort, redirect, render_template, request, url_for


datamanager_bp = Blueprint("datamanager", __name__, url_prefix="/datamanager")


@datamanager_bp.route("/")
def index():
    datamanager = {"name": "Dashboard"}  
    return render_template("datamanager/index.html", datamanager=datamanager)


@datamanager_bp.route("/dashboard/add")
def dashboard_add():
    """
    Render the dashboard add page.
    """

    return render_template("datamanager/dashboard_add.html")


@datamanager_bp.route("/dashboard/config")
def dashboard_config():
    """
    Render the dashboard configuration page.
    """
    return render_template("datamanager/dashboard_config.html")


