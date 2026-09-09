import json
import logging
from urllib.parse import urlencode

import requests

from config.config import get_request_api_endpoint

from application.extensions import cache

from application.data_access.http import (
    fetch_pages_concurrently,
    get_http_session,
    page_timeout,
)
from ..utils import REQUESTS_TIMEOUT

logger = logging.getLogger(__name__)
TOTAL_RESULTS_HEADER = "X-Pagination-Total-Results"


def _requests_url() -> str:
    return f"{get_request_api_endpoint()}/requests"


def _request_url(request_id: str) -> str:
    return f"{get_request_api_endpoint()}/requests/{request_id}"


def _response_details_url(request_id: str) -> str:
    return f"{get_request_api_endpoint()}/requests/{request_id}/response-details"


class AsyncAPIError(Exception):
    """Raised when the async request API returns an error."""

    def __init__(self, message, status_code=None, detail=None):
        self.status_code = status_code
        self.detail = detail
        super().__init__(message)


class ResponseDetailsIncomplete(AsyncAPIError):
    """
    Raised when some response-detail pages failed, carrying the rows that did
    arrive so the caller can still show them. Raising rather than returning the
    short list keeps the memoize cache empty, so the next request retries.
    """

    def __init__(self, message, partial=None):
        super().__init__(message)
        self.partial = partial or []


def submit_request(params: dict) -> str:
    """
    Submit a request to the async API.

    Posts {"params": params} and expects a 202 response.
    Returns the request ID on success.
    Raises AsyncAPIError on any other status.
    """
    payload = {"params": {**params, "service": "manage"}}
    logger.info("Submitting request to async API")
    logger.debug(json.dumps(payload, indent=2))

    response = requests.post(_requests_url(), json=payload, timeout=REQUESTS_TIMEOUT)

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
    response = requests.get(_request_url(request_id), timeout=REQUESTS_TIMEOUT)

    if response.status_code != 200:
        raise AsyncAPIError(
            f"Request {request_id} not found",
            status_code=response.status_code,
        )

    return response.json() or {}


def _total_results(response) -> int:
    """Total rows available, from the API's pagination header, or None if absent."""
    try:
        return int(response.headers[TOTAL_RESULTS_HEADER])
    except Exception:
        return None


def _log_first_batch(batch: list) -> None:
    if not batch:
        return
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


@cache.memoize(timeout=3600)
def fetch_response_details(
    request_id: str,
    limit: int = 500,
    start_offset: int = 0,
    max_rows: int = None,
) -> list:
    """
    Fetch response details for a request, handling pagination.

    The API reports the row count in an X-Pagination-Total-Results header, so once
    the first page is in the rest are fetched in parallel. Falls back to sequential
    paging when that header is missing.

    Raises ResponseDetailsIncomplete, carrying the rows it did get, if any page fails.
    """
    logger.info(
        f"Fetching response details for request_id: {request_id}, "
        f"start_offset={start_offset}, max_rows={max_rows}"
    )
    url = _response_details_url(request_id)
    first_limit = min(limit, max_rows) if max_rows is not None else limit

    try:
        response = get_http_session().get(
            url,
            params={"offset": start_offset, "limit": first_limit},
            timeout=page_timeout(),
        )
        response.raise_for_status()
        first_batch = response.json() or []
    except Exception as e:
        # Returning [] here would cache "this resource has no rows" for the full
        # memoize timeout, and render it with no warning.
        raise ResponseDetailsIncomplete(
            f"First response details page at offset {start_offset} failed for "
            f"{request_id}: {e}"
        ) from e

    if start_offset == 0:
        _log_first_batch(first_batch)

    total_available = _total_results(response)
    if total_available is None:
        logger.warning(
            f"No {TOTAL_RESULTS_HEADER} header on response details for {request_id} — "
            "falling back to sequential paging"
        )
        if len(first_batch) < first_limit:
            # Short page means there is nothing after it, so no need to page at all.
            return first_batch
        return _fetch_response_details_sequentially(
            request_id, limit, start_offset, max_rows, seed=first_batch
        )

    wanted = max(total_available - start_offset, 0)
    if max_rows is not None:
        wanted = min(wanted, max_rows)

    all_details = list(first_batch)
    failed_offsets = []
    offsets = range(start_offset + len(first_batch), start_offset + wanted, limit)
    if offsets:
        urls = [
            f"{url}?{urlencode({'offset': offset, 'limit': limit})}"
            for offset in offsets
        ]
        for offset, page in zip(offsets, fetch_pages_concurrently(urls)):
            if page is None:
                failed_offsets.append(offset)
                continue
            all_details.extend(page)

    all_details = all_details[:wanted]
    # Shortfall also catches pages that returned 200 with fewer rows than expected.
    if failed_offsets or len(all_details) < wanted:
        raise ResponseDetailsIncomplete(
            f"Response details incomplete for {request_id} - got {len(all_details)} "
            f"of {wanted} rows, failed offsets {failed_offsets}",
            partial=all_details,
        )

    logger.info(f"Total response details fetched: {len(all_details)}")
    return all_details


def _fetch_response_details_sequentially(
    request_id: str,
    limit: int,
    start_offset: int,
    max_rows: int,
    seed: list = None,
) -> list:
    """
    Page through response details one request at a time, following batch sizes.

    `seed` carries rows the caller already fetched, so the first page isn't requested
    twice. Raises ResponseDetailsIncomplete if a page fails part way through.
    """
    all_details = list(seed or [])
    offset = start_offset + len(all_details)
    logger.info(
        f"Fetching response details for request_id: {request_id}, "
        f"start_offset={start_offset}, max_rows={max_rows}"
    )

    while True:
        if max_rows is not None and len(all_details) >= max_rows:
            break
        fetch_limit = (
            min(limit, max_rows - len(all_details)) if max_rows is not None else limit
        )
        # Reset per iteration: the handler below reports on it, and a transport
        # failure leaves it unassigned.
        response = None
        try:
            url = _response_details_url(request_id)
            params = {"offset": offset, "limit": fetch_limit}
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

            if offset == 0:
                _log_first_batch(batch)

            all_details.extend(batch)

            if len(batch) < fetch_limit:
                logger.info(f"Last batch received - Total items: {len(all_details)}")
                break

            offset += fetch_limit

        except Exception as e:
            logger.error(f"Response status: {getattr(response, 'status_code', 'N/A')}")
            response_text = getattr(response, "text", "N/A")
            if hasattr(response_text, "__getitem__"):
                logger.error(f"Response text: {response_text[:500]}")
            else:
                logger.error(f"Response text: {response_text}")
            # Returning the rows gathered so far would be memoized as if complete.
            raise ResponseDetailsIncomplete(
                f"Failed to fetch batch at offset {offset} for {request_id}: {e}",
                partial=all_details,
            ) from e

    logger.info(f"Total response details fetched: {len(all_details)}")
    return all_details
