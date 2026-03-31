import csv
import logging
import time
from collections import defaultdict
from io import StringIO

import requests
from flask import current_app

from ..utils import REQUESTS_TIMEOUT

logger = logging.getLogger(__name__)

_cache = {
    "data": None,
    "expires_at": 0,
}
CACHE_TTL_SECONDS = 300  # 5 minutes


def _get_dataset_fields() -> dict[str, list[dict]]:
    """Fetch and cache dataset-field mapping from the specification CSV.

    Returns a dict keyed by dataset ID, each value being a list of field dicts.
    """
    now = time.monotonic()
    if _cache["data"] is not None and now < _cache["expires_at"]:
        return _cache["data"]

    url = current_app.config.get("DATASET_FIELD_CSV_URL")

    try:
        response = requests.get(
            url,
            timeout=REQUESTS_TIMEOUT,
            headers={"User-Agent": "Planning Data - Manage"},
        )
        response.raise_for_status()

        reader = csv.DictReader(StringIO(response.text))
        result: dict[str, list[dict]] = defaultdict(list)
        for row in reader:
            dataset = row.get("dataset", "").strip()
            if dataset:
                result[dataset].append({k: v for k, v in row.items() if k != "dataset"})

    except Exception:
        logger.exception("Error fetching dataset-field CSV")
        if _cache["data"] is not None:
            logger.warning("Returning stale dataset-field cache after fetch failure")
            return _cache["data"]
        raise

    _cache["data"] = dict(result)
    _cache["expires_at"] = now + CACHE_TTL_SECONDS

    return _cache["data"]


def get_fields_for_dataset(dataset_id: str) -> list[dict]:
    """Return all field rows for a given dataset ID, or an empty list if not found."""
    return _get_dataset_fields().get(dataset_id, [])


def get_field_names_for_dataset(dataset_id: str) -> list[str]:
    """Return a sorted list of field names for a given dataset ID."""
    return sorted(row["field"] for row in get_fields_for_dataset(dataset_id))
