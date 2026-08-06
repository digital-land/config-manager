import csv
import hashlib
import logging
import os
import time
from io import StringIO
from urllib.parse import urlsplit, urlunsplit

import requests
from flask import current_app

from ..utils import REQUESTS_TIMEOUT

logger = logging.getLogger(__name__)

_cache = {
    "data": None,
    "expires_at": 0,
    "metadata": {},
}
CACHE_TTL_SECONDS = 300  # 5 minutes


def _url_for_logging(url):
    """Return a URL without query parameters or fragments that may contain secrets."""
    parts = urlsplit(str(url or ""))
    netloc = parts.netloc.rsplit("@", 1)[-1]
    return urlunsplit((parts.scheme, netloc, parts.path, "", ""))


def _get_datasets():
    """Internal: fetch and cache dataset maps.

    Dataset list is sourced from the provision CSV (ground truth for supported
    datasets). Display name and collection are enriched from the specification CSV.
    """

    now = time.monotonic()
    if _cache["data"] is not None and now < _cache["expires_at"]:
        metadata = _cache["metadata"]
        logger.info(
            "Dataset specification cache hit: worker_pid=%s age_seconds=%.1f "
            "expires_in_seconds=%.1f dataset_count=%s configured_dataset_csv_url=%s "
            "resolved_dataset_csv_url=%s",
            os.getpid(),
            max(0, now - metadata.get("loaded_at", now)),
            max(0, _cache["expires_at"] - now),
            metadata.get("dataset_count", "unknown"),
            metadata.get("configured_dataset_csv_url", "unknown"),
            metadata.get("resolved_dataset_csv_url", "unknown"),
        )
        return _cache["data"]
    try:
        # Step 1: get unique dataset IDs from the provision CSV
        provision_url = current_app.config.get("PROVISION_CSV_URL")
        dataset_csv_url = current_app.config.get("DATASET_CSV_URL")
        logger.info(
            "Refreshing dataset specification cache: worker_pid=%s "
            "stale_cache_available=%s provision_csv_url=%s dataset_csv_url=%s",
            os.getpid(),
            _cache["data"] is not None,
            _url_for_logging(provision_url),
            _url_for_logging(dataset_csv_url),
        )
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
        logger.info(
            "Fetched provision CSV: worker_pid=%s status=%s resolved_url=%s "
            "response_bytes=%s dataset_count=%s",
            os.getpid(),
            prov_response.status_code,
            _url_for_logging(prov_response.url),
            len(prov_response.content),
            len(provision_dataset_ids),
        )

        # Step 2: fetch name + collection from the specification CSV (source of
        # truth - reflects new datasets immediately without waiting for the
        # overnight pipeline run)
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
        logger.info(
            "Fetched dataset specification CSV: worker_pid=%s status=%s "
            "resolved_url=%s response_bytes=%s dataset_count=%s etag=%r "
            "last_modified=%r sha256=%s",
            os.getpid(),
            spec_response.status_code,
            _url_for_logging(spec_response.url),
            len(spec_response.content),
            len(spec_lookup),
            spec_response.headers.get("ETag"),
            spec_response.headers.get("Last-Modified"),
            hashlib.sha256(spec_response.content).hexdigest(),
        )

        name_to_dataset_id = {}
        name_to_collection_id = {}
        dataset_id_to_name = {}
        dataset_id_to_typology = {}
        for dataset_id in provision_dataset_ids:
            spec_entry = spec_lookup.get(dataset_id)
            name = (spec_entry.get("name") if spec_entry else None) or dataset_id
            collection = (
                spec_entry.get("collection") if spec_entry else None
            ) or dataset_id
            typology = (spec_entry.get("typology") if spec_entry else None) or ""

            name_to_dataset_id[name] = dataset_id
            name_to_collection_id[name] = collection
            dataset_id_to_name[dataset_id] = name
            dataset_id_to_typology[dataset_id] = typology

        dataset_options = sorted(name_to_dataset_id.keys())
    except Exception as e:
        logger.exception("Error fetching datasets")
        if _cache["data"] is not None:
            metadata = _cache["metadata"]
            logger.warning(
                "Returning stale dataset cache after fetch failure: worker_pid=%s "
                "age_seconds=%.1f dataset_count=%s configured_dataset_csv_url=%s "
                "resolved_dataset_csv_url=%s",
                os.getpid(),
                max(0, now - metadata.get("loaded_at", now)),
                metadata.get("dataset_count", "unknown"),
                metadata.get("configured_dataset_csv_url", "unknown"),
                metadata.get("resolved_dataset_csv_url", "unknown"),
            )
            return _cache["data"]
        raise Exception("Failed to fetch dataset list") from e

    result = (
        dataset_options,
        name_to_dataset_id,
        name_to_collection_id,
        dataset_id_to_name,
        dataset_id_to_typology,
    )
    _cache["data"] = result
    _cache["expires_at"] = now + CACHE_TTL_SECONDS
    _cache["metadata"] = {
        "loaded_at": now,
        "dataset_count": len(dataset_id_to_typology),
        "provision_dataset_ids": provision_dataset_ids,
        "specification_dataset_ids": set(spec_lookup),
        "configured_dataset_csv_url": _url_for_logging(dataset_csv_url),
        "resolved_dataset_csv_url": _url_for_logging(spec_response.url),
        "dataset_csv_etag": spec_response.headers.get("ETag"),
        "dataset_csv_last_modified": spec_response.headers.get("Last-Modified"),
        "dataset_csv_sha256": hashlib.sha256(spec_response.content).hexdigest(),
    }
    logger.info(
        "Dataset specification cache refreshed: worker_pid=%s ttl_seconds=%s "
        "dataset_count=%s",
        os.getpid(),
        CACHE_TTL_SECONDS,
        len(dataset_id_to_typology),
    )

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


def get_dataset_typology(dataset_id: str) -> str:
    """Return the typology for a dataset (e.g. 'geography'), or '' if unknown."""
    typology_map = _get_datasets()[4]
    typology = typology_map.get(dataset_id, "")
    metadata = _cache["metadata"]
    now = time.monotonic()
    logger.info(
        "Dataset typology lookup: worker_pid=%s dataset_id=%r typology=%r "
        "in_dataset_map=%s in_provision=%s in_specification=%s "
        "cache_age_seconds=%.1f cache_expired=%s configured_dataset_csv_url=%s "
        "resolved_dataset_csv_url=%s dataset_csv_etag=%r "
        "dataset_csv_last_modified=%r dataset_csv_sha256=%s",
        os.getpid(),
        dataset_id,
        typology,
        dataset_id in typology_map,
        dataset_id in metadata.get("provision_dataset_ids", set()),
        dataset_id in metadata.get("specification_dataset_ids", set()),
        max(0, now - metadata.get("loaded_at", now)),
        now >= _cache["expires_at"],
        metadata.get("configured_dataset_csv_url", "unknown"),
        metadata.get("resolved_dataset_csv_url", "unknown"),
        metadata.get("dataset_csv_etag"),
        metadata.get("dataset_csv_last_modified"),
        metadata.get("dataset_csv_sha256", "unknown"),
    )
    return typology


def search_datasets(query: str, limit: int = 10) -> list:
    """Search dataset names matching a query string (case-insensitive)."""
    query_lower = query.lower()
    return [name for name in get_dataset_options() if query_lower in name.lower()][
        :limit
    ]
