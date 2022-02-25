from flask import Blueprint, jsonify, redirect, render_template, url_for

from application.blueprints.resource.forms import SearchForm
from application.models import Resource

resource_bp = Blueprint("resource", __name__, url_prefix="/resource")


@resource_bp.route("/", methods=["GET", "POST"])
def search():
    form = SearchForm()
    if form.validate_on_submit():
        resource_hash = form.resource.data.strip()
        resource = Resource.query.get(resource_hash)
        if resource:
            return redirect(
                url_for("resource.resource", resource_hash=resource.resource)
            )
        form.resource.errors.append("We don't recognise that hash, try another")

    resources = (
        Resource.query.filter(Resource.start_date != None)  # noqa: E711
        .order_by(Resource.start_date.desc())
        .limit(5)
        .all()
    )

    return render_template("resource/search.html", form=form, resources=resources)


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
