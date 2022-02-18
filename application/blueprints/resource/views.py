from flask import Blueprint, render_template

from application.models import Resource

resource_bp = Blueprint("resource", __name__, url_prefix="/resource")


@resource_bp.route("/<resource_hash>")
def resource(resource_hash):
    resource = Resource.query.get(resource_hash)
    return render_template("resource/resource.html", resource=resource)
