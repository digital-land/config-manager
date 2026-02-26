import json
import logging

import requests

from ..config import (
    get_async_request_url,
    get_async_requests_url,
    get_async_response_details_url,
)
from ..utils import REQUESTS_TIMEOUT

logger = logging.getLogger(__name__)


class AsyncAPIError(Exception):
    """Raised when the async request API returns an error."""

    def __init__(self, message, status_code=None, detail=None):
        self.status_code = status_code
        self.detail = detail
        super().__init__(message)


def submit_request(params: dict) -> str:
    """
    Submit a request to the async API.

    Posts {"params": params} and expects a 202 response.
    Returns the request ID on success.
    Raises AsyncAPIError on any other status.
    """
    payload = {"params": params}
    logger.info("Submitting request to async API")
    logger.debug(json.dumps(payload, indent=2))

    response = requests.post(
        get_async_requests_url(), json=payload, timeout=REQUESTS_TIMEOUT
    )

    logger.info(f"Async API responded with {response.status_code}")
    try:
        logger.debug(json.dumps(response.json(), indent=2))
    except Exception:
        logger.debug((response.text or "")[:2000])

    if response.status_code == 202:
        request_id = response.json().get("id")
        logger.info(f"Request created with ID: {request_id}")
        return request_id

    try:
        detail = response.json()
    except Exception:
        detail = response.text

    raise AsyncAPIError(
        f"Request submission failed ({response.status_code})",
        status_code=response.status_code,
        detail=detail,
    )


def fetch_request(request_id: str) -> dict:
    """
    Fetch a request by ID from the async API.

    Returns the parsed JSON response on 200.
    Raises AsyncAPIError on non-200 status.
    """
    response = requests.get(get_async_request_url(request_id), timeout=REQUESTS_TIMEOUT)

    if response.status_code != 200:
        raise AsyncAPIError(
            f"Request {request_id} not found",
            status_code=response.status_code,
        )

    return response.json() or {}


def fetch_response_details(request_id: str, limit: int = 50) -> list:
    """
    Fetch all response details for a request, handling pagination.

    Makes repeated GET requests with offset/limit until all pages are fetched.
    Returns the aggregated list of response detail items.
    """
    all_details = []
    offset = 0
    logger.info(f"Fetching response details for request_id: {request_id}")

    while True:
        try:
            url = get_async_response_details_url(request_id)
            params = {"offset": offset, "limit": limit}
            logger.debug(f"Fetching batch - URL: {url}, Params: {params}")

            response = requests.get(url, params=params, timeout=REQUESTS_TIMEOUT)
            content_length = getattr(response, "content", None)
            content_length = (
                len(content_length) if content_length is not None else "N/A"
            )
            logger.info(
                f"Batch response - Status: {response.status_code}, Content-Length: {content_length}"
            )

            response.raise_for_status()
            batch = response.json() or []
            logger.info(f"Batch parsed - Items: {len(batch)}")

            if not batch:
                logger.info("No more batches available")
                break

            # Log sample of first batch for debugging
            if offset == 0 and batch:
                logger.info(
                    f"First batch sample - Item keys: {list(batch[0].keys()) if batch[0] else 'Empty item'}"
                )
                if batch[0] and "converted_row" in batch[0]:
                    converted_sample = batch[0]["converted_row"]
                    if converted_sample:
                        logger.info(
                            f"First converted_row sample: {dict(list(converted_sample.items())[:3])}"
                        )
                    else:
                        logger.info("Empty converted_row")

            all_details.extend(batch)

            if len(batch) < limit:
                logger.info(f"Last batch received - Total items: {len(all_details)}")
                break

            offset += limit

        except Exception as e:
            logger.error(f"Failed to fetch batch at offset {offset}: {e}")
            logger.error(f"Response status: {getattr(response, 'status_code', 'N/A')}")
            response_text = getattr(response, "text", "N/A")
            if hasattr(response_text, "__getitem__"):
                logger.error(f"Response text: {response_text[:500]}")
            else:
                logger.error(f"Response text: {response_text}")
            break

    logger.info(f"Total response details fetched: {len(all_details)}")
    return all_details
