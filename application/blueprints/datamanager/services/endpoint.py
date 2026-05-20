import logging

import requests
from flask import current_app

from ..utils import REQUESTS_TIMEOUT

logger = logging.getLogger(__name__)


def get_endpoint_urls_for_hashes(hashes: list) -> dict:
    """
    Given a list of endpoint hashes, returns a dict mapping {hash: endpoint_url}
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
                    "end_date": row.get("end_date") or "",
                }
    except Exception as e:
        logger.error(f"Failed to fetch endpoint URLs for hashes: {e}", exc_info=True)

    return result
