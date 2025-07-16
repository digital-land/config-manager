import datetime

from flask import Blueprint, abort, redirect, render_template, request, url_for


datamanager_bp = Blueprint("datamanager", __name__, url_prefix="/datamanager")


@datamanager_bp.get("/")
def index():
    """
    Render the main datamanager page.
    """
    return render_template("datamanager/index.html")