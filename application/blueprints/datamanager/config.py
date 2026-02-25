from flask import current_app

from config.config import get_request_api_endpoint

# --- Planning Data API URLs ---
# TODO: This is temporary!, this is helpful to move all url's to be retrieved
# from the overall config.py for Config-manager


def get_planning_base_url():
    return current_app.config["PLANNING_BASE_URL"]


def get_datasets_url():
    return f"{get_planning_base_url()}/dataset.json?_labels=on&_size=max"


def get_entity_search_url(dataset_id, reference):
    return f"{get_planning_base_url()}/entity.json?dataset={dataset_id}&reference={reference}"


def get_entity_geojson_url(reference):
    return f"{get_planning_base_url()}/entity.geojson?reference={reference}"


# --- Async Request API URLs ---


def get_async_requests_url():
    return f"{get_request_api_endpoint()}/requests"


def get_async_request_url(request_id):
    return f"{get_request_api_endpoint()}/requests/{request_id}"


def get_async_response_details_url(request_id):
    return f"{get_request_api_endpoint()}/requests/{request_id}/response-details"
