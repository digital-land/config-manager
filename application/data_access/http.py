import logging
from concurrent.futures import ThreadPoolExecutor

import requests
from flask import current_app
from requests.adapters import HTTPAdapter

logger = logging.getLogger(__name__)

# Fallbacks for when there is no application context to read config from.
DEFAULT_MAX_WORKERS = 8
DEFAULT_PAGE_TIMEOUT = 10

_session = None


def _config(key: str, default: int) -> int:
    try:
        return current_app.config.get(key, default)
    except RuntimeError:
        return default


def page_max_workers() -> int:
    return _config("HTTP_PAGE_MAX_WORKERS", DEFAULT_MAX_WORKERS)


def page_timeout() -> int:
    return _config("HTTP_PAGE_TIMEOUT", DEFAULT_PAGE_TIMEOUT)


def get_http_session() -> requests.Session:
    """
    The process-wide pooled session used for paged fetches. Reusing connections
    avoids a TLS handshake per page; the pool has to hold at least one connection
    per worker or urllib3 discards them.
    """
    global _session
    if _session is None:
        session = requests.Session()
        adapter = HTTPAdapter(
            pool_connections=4, pool_maxsize=max(page_max_workers() * 2, 16)
        )
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        _session = session
    return _session


def fetch_pages_concurrently(
    urls: list,
    max_workers: int = None,
    timeout: int = None,
    headers: dict = None,
    allow_redirects: bool = True,
) -> list:
    """
    GET every url and return the parsed JSON bodies in the order the urls were given.
    A page that fails contributes None rather than raising, so one bad page doesn't
    lose the others.

    Callers build their own paginated urls, fully resolved — the workers run off the
    request thread and so have no Flask application context to read config from.
    """
    if not urls:
        return []

    session = get_http_session()
    timeout = timeout if timeout is not None else page_timeout()
    max_workers = max_workers if max_workers is not None else page_max_workers()

    def _get(url):
        try:
            response = session.get(
                url, timeout=timeout, headers=headers, allow_redirects=allow_redirects
            )
            if response.status_code != 200:
                raise requests.HTTPError(
                    f"Unexpected page status: {response.status_code}"
                )
            return response.json()
        except Exception as e:
            logger.error(f"Failed to fetch page {url}: {e}", exc_info=True)
            return None

    with ThreadPoolExecutor(max_workers=min(max_workers, len(urls))) as executor:
        # map preserves input order, which callers rely on to reassemble by offset.
        return list(executor.map(_get, urls))
