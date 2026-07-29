import logging

import requests
from flask import current_app

from ..utils import REQUESTS_TIMEOUT

logger = logging.getLogger(__name__)


def get_endpoint_info_for_hashes(hashes: list) -> dict:
    """
    Given a list of endpoint hashes, returns a dict mapping
    {hash: {"endpoint_url": ..., "entry_date": ..., "end_date": ...}}
    by querying the datasette endpoint table.
    """
    if not hashes:
        return {}

    datasette_url = current_app.config.get("DATASETTE_BASE_URL")
    url = (
        f"{datasette_url}/endpoint.json"
        f"?endpoint__in={','.join(hashes)}"
        f"&_shape=objects"
        f"&_size=max"
    )

    result = {}
    try:
        response = requests.get(url, timeout=REQUESTS_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        for row in data.get("rows", []):
            h = row.get("endpoint")
            if h:
                result[h] = {
                    "endpoint_url": row.get("endpoint_url", ""),
                    "entry_date": row.get("entry_date") or "",
                    "end_date": row.get("end_date") or "",
                }
    except Exception as e:
        logger.error(f"Failed to fetch endpoint URLs for hashes: {e}", exc_info=True)

    return result


def get_endpoint_log_summary_for_hashes(hashes: list) -> dict:
    """
    Given a list of endpoint hashes, returns a dict mapping
    {hash: {"latest_status": ..., "latest_log_entry_date": ...}}.
    """
    if not hashes:
        return {}

    datasette_url = current_app.config.get("DATASETTE_BASE_URL")
    datasette_root = datasette_url.rsplit("/", 1)[0]
    url = (
        f"{datasette_root}/performance/reporting_historic_endpoints.json"
        f"?endpoint__in={','.join(hashes)}"
        f"&_shape=objects"
        f"&_size=max"
    )

    result = {}
    try:
        response = requests.get(url, timeout=REQUESTS_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        for row in data.get("rows", []):
            h = row.get("endpoint")
            if not h:
                continue
            log_date = row.get("latest_log_entry_date") or ""
            existing = result.get(h)
            # Keep the most recent record per endpoint (dates are ISO, so string
            # comparison orders them correctly).
            if existing is None or log_date > existing["latest_log_entry_date"]:
                result[h] = {
                    "latest_status": row.get("latest_status") or "",
                    "latest_log_entry_date": log_date,
                }
    except Exception as e:
        logger.error(
            f"Failed to fetch endpoint log summary for hashes: {e}", exc_info=True
        )

    return result
