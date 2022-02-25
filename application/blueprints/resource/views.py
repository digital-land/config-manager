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


@resource_bp.route("/<resource_hash>/columns")
def columns(resource_hash):
    # To do: get columns/attr names from the original resource
    # To do: get expected/allowable attributes from schema
    # To do: get mappings between columns and expected columns
    # To do: link to somewhere to edit mappings
    return render_template("resource/columns.html")


@resource_bp.route("/<resource_hash>/values")
def values(resource_hash):
    # To do: get expected/allowable attributes from schema and check if any contain specific values (category fields)
    # To do: get allowable values
    # To do: check values in resource against allowable values
    # To do: link to somewhere to edit mappings
    return render_template("resource/values.html")
