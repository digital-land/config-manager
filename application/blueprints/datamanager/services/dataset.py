import logging
import time

import requests

from ..config import get_datasets_url
from ..utils import REQUESTS_TIMEOUT

logger = logging.getLogger(__name__)

_cache = {
    "data": None,
    "expires_at": 0,
}
CACHE_TTL_SECONDS = 300  # 5 minutes


def _get_datasets():
    """Internal: fetch and cache the dataset maps."""
    now = time.monotonic()
    if _cache["data"] is not None and now < _cache["expires_at"]:
        return _cache["data"]

    try:
        ds_response = requests.get(
            get_datasets_url(),
            timeout=REQUESTS_TIMEOUT,
            headers={"User-Agent": "Planning Data - Manage"},
        ).json()
    except Exception as e:
        logger.exception("Error fetching datasets")
        if _cache["data"] is not None:
            logger.warning("Returning stale dataset cache after fetch failure")
            return _cache["data"]
        raise Exception("Failed to fetch dataset list") from e

    datasets = [d for d in ds_response.get("datasets", []) if "collection" in d]
    dataset_options = sorted([d["name"] for d in datasets])
    name_to_dataset_id = {d["name"]: d["dataset"] for d in datasets}
    name_to_collection_id = {d["name"]: d["collection"] for d in datasets}
    dataset_id_to_name = {d["dataset"]: d["name"] for d in datasets}

    result = (
        datasets,
        dataset_options,
        name_to_dataset_id,
        name_to_collection_id,
        dataset_id_to_name,
    )

    _cache["data"] = result
    _cache["expires_at"] = now + CACHE_TTL_SECONDS

    return result


def get_dataset_options() -> list:
    """Return sorted list of dataset names for autocomplete."""
    return _get_datasets()[1]


def get_dataset_id(name: str) -> str | None:
    """Look up the dataset ID for a given dataset name."""
    return _get_datasets()[2].get(name)


def get_collection_id(name: str) -> str | None:
    """Look up the collection ID for a given dataset name."""
    return _get_datasets()[3].get(name)


def get_dataset_name(dataset_id: str, default: str = None) -> str | None:
    """Look up the dataset name for a given dataset ID."""
    return _get_datasets()[4].get(dataset_id, default)


def search_datasets(query: str, limit: int = 10) -> list:
    """Search dataset names matching a query string (case-insensitive)."""
    query_lower = query.lower()
    return [name for name in get_dataset_options() if query_lower in name.lower()][
        :limit
    ]
