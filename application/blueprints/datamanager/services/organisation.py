import csv
import logging
import time
from io import StringIO

import requests
from flask import current_app

from ..utils import REQUESTS_TIMEOUT

logger = logging.getLogger(__name__)

_provision_cache = {}
_PROVISION_TTL = 300  # 5 minutes

_org_mapping_cache = {
    "data": None,
    "expires_at": 0,
}
_ORG_MAPPING_TTL = 600  # 10 minutes — changes very rarely


def get_provision_orgs_for_dataset(dataset_id: str) -> list:
    """
    Fetch organisation codes for a given dataset from the provision CSV.
    Results are cached per dataset_id for 5 minutes.
    Returns a list of organisation codes.
    """
    now = time.monotonic()
    cached = _provision_cache.get(dataset_id)
    if cached and now < cached["expires_at"]:
        return cached["data"]

    try:
        provision_url = current_app.config.get("PROVISION_CSV_URL")
        response = requests.get(provision_url, timeout=REQUESTS_TIMEOUT)
        response.raise_for_status()
        reader = csv.DictReader(StringIO(response.text))
        orgs = []
        seen = set()
        for row in reader:
            if row.get("dataset") == dataset_id:
                org_code = row.get("organisation", "")
                if org_code and org_code not in seen:
                    orgs.append(org_code)
                    seen.add(org_code)

        _provision_cache[dataset_id] = {
            "data": orgs,
            "expires_at": now + _PROVISION_TTL,
        }
        return orgs
    except Exception as e:
        logger.error(f"Failed to fetch provision orgs for dataset: {e}")
        if cached:
            return cached["data"]
        return []


def _get_org_mapping() -> dict:
    """
    Internal: build and cache the full organisation code -> name mapping.
    Fetches from the organisation.json datasette endpoint.
    Handles pagination. Cached for 10 minutes.
    """
    now = time.monotonic()
    if _org_mapping_cache["data"] is not None and now < _org_mapping_cache["expires_at"]:
        return _org_mapping_cache["data"]

    org_mapping = {}
    try:
        datasette_url = current_app.config.get("DATASETTE_BASE_URL")
        url = f"{datasette_url}/organisation.json?_shape=objects&_size=max"

        page_count = 0
        while url:
            page_count += 1

            response = requests.get(url, timeout=REQUESTS_TIMEOUT)
            response.raise_for_status()
            data = response.json()

            rows = data.get("rows", []) if isinstance(data, dict) else data

            for row in rows:
                if isinstance(row, dict):
                    code = row.get("organisation")
                    name = row.get("name")
                    if code and name:
                        org_mapping[code] = name

            url = data.get("next_url") if isinstance(data, dict) else None
            if url and url.startswith("/"):
                url = f"{datasette_url.rstrip('/')}{url}"

        logger.info(
            f"Built organisation mapping with {len(org_mapping)} entries from {page_count} page(s)"
        )

        _org_mapping_cache["data"] = org_mapping
        _org_mapping_cache["expires_at"] = now + _ORG_MAPPING_TTL

    except Exception as e:
        logger.error(f"Failed to fetch organisation mapping: {e}", exc_info=True)
        if _org_mapping_cache["data"] is not None:
            logger.warning("Returning stale organisation mapping after fetch failure")
            return _org_mapping_cache["data"]

    return org_mapping


def get_organisation_name(code: str) -> str:
    """Look up the display name for an organisation code.
    Returns the name, or the code itself if not found."""
    mapping = _get_org_mapping()
    return mapping.get(code, code)


def is_valid_organisation(code: str) -> bool:
    """Check whether an organisation code exists."""
    return code in _get_org_mapping()


def format_org_options(org_codes: list) -> list:
    """Format a list of org codes as dicts with 'code' and 'label' keys."""
    mapping = _get_org_mapping()
    return [
        {"code": code, "label": f"{mapping.get(code, code)} ({code})"}
        for code in org_codes
    ]
