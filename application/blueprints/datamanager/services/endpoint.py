import logging
from urllib.parse import quote

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
    {hash: {"latest_status": ..., "latest_log_entry_date": ...,
            "latest_200_date": ...}} by querying the datasette ``log`` table.

    Warning the ``log`` table is large and slow
    """
    if not hashes:
        return {}

    datasette_url = current_app.config.get("DATASETTE_BASE_URL")
    hash_list = ",".join(f"'{h}'" for h in hashes)
    sql = (
        "SELECT l.endpoint AS endpoint, "
        "l.status AS latest_status, "
        "l.entry_date AS latest_log_entry_date, "
        "(SELECT max(l2.entry_date) FROM log l2 "
        "WHERE l2.endpoint = l.endpoint AND l2.status = '200') AS latest_200_date "
        "FROM log l "
        "WHERE l.rowid IN ("
        f"SELECT max(rowid) FROM log WHERE endpoint IN ({hash_list}) GROUP BY endpoint)"
    )
    url = f"{datasette_url}.json?sql={quote(sql)}&_shape=array&_size=max"

    result = {}
    try:
        response = requests.get(url, timeout=REQUESTS_TIMEOUT)
        response.raise_for_status()
        rows = response.json()
        # _shape=array returns a list of row dicts.
        for row in rows or []:
            h = row.get("endpoint")
            if h:
                result[h] = {
                    "latest_status": row.get("latest_status") or "",
                    "latest_log_entry_date": row.get("latest_log_entry_date") or "",
                    "latest_200_date": row.get("latest_200_date") or "",
                }
    except Exception as e:
        logger.error(
            f"Failed to fetch endpoint log summary for hashes: {e}", exc_info=True
        )

    return result
