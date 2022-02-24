from flask import Blueprint, jsonify, render_template

from application.blueprints.resource.forms import SearchForm
from application.models import Resource

resource_bp = Blueprint("resource", __name__, url_prefix="/resource")


@resource_bp.route("/")
def index():
    form = SearchForm()
    return render_template("resource/search.html", form=form)


@resource_bp.route("/<resource_hash>")
def resource(resource_hash):
    resource = Resource.query.get(resource_hash)
    return render_template("resource/resource.html", resource=resource)


@resource_bp.route("/<resource_hash>.json")
def resource_json(resource_hash):
    resource = Resource.query.get(resource_hash)
    if resource:
        return jsonify(resource), 200
    return {}, 404
