import logging
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse

import requests

from application.extensions import cache

logger = logging.getLogger(__name__)

_DOC_CRAWLER_TIMEOUT = 10


def is_gov_uk_url(url):
    """Return True if url's hostname is gov.uk or a subdomain of gov.uk."""
    if not url:
        return False
    try:
        hostname = urlparse(url).hostname or ""
        return hostname == "gov.uk" or hostname.endswith(".gov.uk")
    except Exception:
        return False


class _HrefCollector(HTMLParser):
    def __init__(self):
        super().__init__()
        self.hrefs = []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            for name, value in attrs:
                if name == "href" and value:
                    self.hrefs.append(value.strip())


def _hrefs_match(href, endpoint_url, documentation_url):
    """Return True if href resolves to endpoint_url under any matching strategy."""
    endpoint_parsed = urlparse(endpoint_url)
    endpoint_path = endpoint_parsed.path

    # Exact full URL match
    if href == endpoint_url:
        return True

    # Resolve relative href against documentation URL and compare
    try:
        resolved = urljoin(documentation_url, href)
        if resolved == endpoint_url:
            return True
    except Exception:
        pass

    # Path-only match (e.g. /downloads/file/3149/... == endpoint path)
    if href == endpoint_path:
        return True

    # Compare the path portion of an absolute href
    try:
        href_parsed = urlparse(href)
        if href_parsed.path and href_parsed.path == endpoint_path:
            return True
    except Exception:
        pass

    return False


@cache.memoize(timeout=3600)
def check_endpoint_in_doc(documentation_url, endpoint_url):
    """
    Fetch documentation_url and check whether endpoint_url is linked from it.

    Returns a dict:
        found (bool): True if the endpoint URL was found in the page's links
        matched_href (str|None): the raw href value that matched, if any
        error (str|None): description of any fetch/parse error
    """
    if not documentation_url or not endpoint_url:
        return {"found": False, "matched_href": None, "error": "Missing URL"}

    try:
        resp = requests.get(
            documentation_url,
            timeout=_DOC_CRAWLER_TIMEOUT,
            allow_redirects=True,
            headers={"User-Agent": "digital-land-config-manager/1.0"},
        )
        resp.raise_for_status()
    except requests.exceptions.Timeout:
        logger.warning("Timeout fetching doc URL: %s", documentation_url)
        return {"found": False, "matched_href": None, "error": "Request timed out"}
    except requests.exceptions.RequestException as exc:
        logger.warning("Error fetching doc URL %s: %s", documentation_url, exc)
        return {"found": False, "matched_href": None, "error": str(exc)}

    collector = _HrefCollector()
    try:
        collector.feed(resp.text)
    except Exception as exc:
        logger.warning("Error parsing HTML from %s: %s", documentation_url, exc)
        return {"found": False, "matched_href": None, "error": "HTML parse error"}

    for href in collector.hrefs:
        if _hrefs_match(href, endpoint_url, documentation_url):
            return {"found": True, "matched_href": href, "error": None}

    return {"found": False, "matched_href": None, "error": None}
