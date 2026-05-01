import csv
import logging
import time
from io import StringIO

import requests
from flask import current_app

from ..config import get_datasets_url
from ..utils import REQUESTS_TIMEOUT

logger = logging.getLogger(__name__)

_cache = {
    "data": None,
    "expires_at": 0,
}
CACHE_TTL_SECONDS = 300  # 5 minutes


def _get_datasets():
    now = time.monotonic()
    if _cache["data"] is not None and now < _cache["expires_at"]:
        return _cache["data"]
    try:
        # Step 1: get unique dataset IDs from the provision CSV
        provision_url = current_app.config.get("PROVISION_CSV_URL")
        prov_response = requests.get(
            provision_url,
            timeout=REQUESTS_TIMEOUT,
            headers={"User-Agent": "Planning Data - Manage"},
        )
        prov_response.raise_for_status()
        reader = csv.DictReader(StringIO(prov_response.text))
        provision_dataset_ids = {
            row["dataset"].strip() for row in reader if row.get("dataset", "").strip()
        }

        # Step 2: fetch name + collection from the specification CSV (source of
        # truth - reflects new datasets immediately without waiting for the
        # overnight pipeline run)
        dataset_csv_url = current_app.config.get("DATASET_CSV_URL")
        spec_response = requests.get(
            dataset_csv_url,
            timeout=REQUESTS_TIMEOUT,
            headers={"User-Agent": "Planning Data - Manage"},
        )
        spec_response.raise_for_status()
        spec_reader = csv.DictReader(StringIO(spec_response.text))
        spec_lookup = {
            row["dataset"].strip(): row
            for row in spec_reader
            if row.get("dataset", "").strip()
        }

        name_to_dataset_id = {}
        name_to_collection_id = {}
        dataset_id_to_name = {}
        for dataset_id in provision_dataset_ids:
            spec_entry = spec_lookup.get(dataset_id)
            name = (
                spec_entry.get("name") if spec_entry else None
            ) or dataset_id
            collection = (
                spec_entry.get("collection") if spec_entry else None
            ) or dataset_id

            name_to_dataset_id[name] = dataset_id
            name_to_collection_id[name] = collection
            dataset_id_to_name[dataset_id] = name

        dataset_options = sorted(name_to_dataset_id.keys())
    except Exception as e:
        logger.exception("Error fetching datasets")
        if _cache["data"] is not None:
            logger.warning("Returning stale dataset cache after fetch failure")
            return _cache["data"]
        raise Exception("Failed to fetch dataset list") from e

    result = (
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
    return _get_datasets()[0]


def get_dataset_id(name: str) -> str | None:
    """Look up the dataset ID for a given dataset name."""
    return _get_datasets()[1].get(name)


def get_collection_id(name: str) -> str | None:
    """Look up the collection ID for a given dataset name."""
    return _get_datasets()[2].get(name)


def get_dataset_name(dataset_id: str, default: str = None) -> str | None:
    """Look up the dataset name for a given dataset ID."""
    return _get_datasets()[3].get(dataset_id, default)


def search_datasets(query: str, limit: int = 10) -> list:
    """Search dataset names matching a query string (case-insensitive)."""
    query_lower = query.lower()
    return [name for name in get_dataset_options() if query_lower in name.lower()][
        :limit
    ]
