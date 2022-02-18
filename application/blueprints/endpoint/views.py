from flask import Blueprint, jsonify, request

from application.models import Endpoint

endpoint_bp = Blueprint("endpoint", __name__, url_prefix="/endpoint")


@endpoint_bp.route("/<endpoint>")
def endpoint_json(endpoint):
    endpoint = Endpoint.query.get(endpoint)
    if endpoint:
        return jsonify(endpoint), 200
    return {}, 404


@endpoint_bp.route("/search", methods=["POST"])
def search_json():
    data = request.json
    if "endpoint_url" in data.keys():
        endpoint = Endpoint.query.filter(
            Endpoint.endpoint_url == data["endpoint_url"]
        ).first()
        if endpoint:
            return jsonify(endpoint), 200
    return {}, 404
