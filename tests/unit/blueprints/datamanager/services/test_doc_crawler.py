from unittest.mock import patch, Mock

import requests as requests_lib

from application.blueprints.datamanager.services.doc_crawler import (
    _hrefs_match,
    check_endpoint_in_doc,
    is_gov_uk_url,
)

# Use unique (doc_url, endpoint_url) pairs per test to avoid @cache.memoize
# collisions across tests (app fixture is session-scoped, so cache persists).
DOC_URL = "https://www.example.gov.uk/data/"
NON_GOV_DOC_URL = "https://www.council.org.uk/data/"
ENDPOINT_URL = "https://maps.example.gov.uk/server/rest/services/data.geojson"


class TestIsGovUkUrl:
    def test_gov_uk_apex(self):
        assert is_gov_uk_url("https://gov.uk/data") is True

    def test_subdomain_of_gov_uk(self):
        assert is_gov_uk_url("https://www.example.gov.uk/page") is True

    def test_non_gov_uk_domain(self):
        assert is_gov_uk_url("https://www.council.org.uk/page") is False

    def test_empty_string(self):
        assert is_gov_uk_url("") is False

    def test_none(self):
        assert is_gov_uk_url(None) is False

    def test_gov_uk_in_path_not_hostname(self):
        # hostname is arcgis.com, not gov.uk
        assert is_gov_uk_url("https://arcgis.com/gov.uk/stuff") is False


class TestHrefsMatch:
    def test_exact_match(self):
        assert _hrefs_match(ENDPOINT_URL, ENDPOINT_URL, DOC_URL) is True

    def test_relative_href_resolves_to_endpoint(self):
        # relative path resolved against DOC_URL gives ENDPOINT_URL
        relative = "/server/rest/services/data.geojson"
        resolved_endpoint = (
            "https://www.example.gov.uk/server/rest/services/data.geojson"
        )
        assert _hrefs_match(relative, resolved_endpoint, DOC_URL) is True

    def test_path_only_match(self):
        endpoint = "https://maps.example.gov.uk/downloads/file/3149/data.csv"
        assert _hrefs_match("/downloads/file/3149/data.csv", endpoint, DOC_URL) is True

    def test_absolute_href_path_matches_endpoint_path(self):
        href = "https://other.example.gov.uk/server/rest/services/data.geojson"
        assert _hrefs_match(href, ENDPOINT_URL, DOC_URL) is True

    def test_no_match(self):
        assert (
            _hrefs_match("https://unrelated.com/other", ENDPOINT_URL, DOC_URL) is False
        )


def _mock_response(text="", status_code=200, raise_for=None):
    resp = Mock()
    resp.text = text
    resp.status_code = status_code
    if raise_for:
        resp.raise_for_status.side_effect = raise_for
    else:
        resp.raise_for_status.return_value = None
    return resp


class TestCheckEndpointInDoc:
    """
    Covers the five accordion colour-logic scenarios:

    | Doc gov.uk? | Found? | Access error? | Endpoint gov.uk? | Colour |
    |-------------|--------|---------------|------------------|--------|
    | any         | any    | any           | —  (no URL)      | —      |  → Missing URL error
    | ✗           | any    | any           | any              | Red    |  → found/not-found, non-gov.uk doc
    | ✓           | ✓      | —             | —                | Green  |  → found
    | ✓           | ✗      | ✓             | —                | Orange |  → access error
    | ✓           | ✗      | ✗             | ✓                | Orange |  → not found, gov.uk endpoint
    | ✓           | ✗      | ✗             | ✗                | Red    |  → not found, non-gov.uk endpoint
    """

    def test_missing_documentation_url_returns_error(self, app):
        with app.app_context():
            result = check_endpoint_in_doc("", ENDPOINT_URL)
        assert result["found"] is False
        assert result["error"] == "Missing URL"

    def test_missing_endpoint_url_returns_error(self, app):
        with app.app_context():
            result = check_endpoint_in_doc(DOC_URL, "")
        assert result["found"] is False
        assert result["error"] == "Missing URL"

    def test_endpoint_found_in_doc_green(self, app):
        """doc gov.uk + endpoint found → Green"""
        doc = "https://www.example.gov.uk/data/found/"
        ep = "https://maps.example.gov.uk/layer-found.geojson"
        html = f'<html><body><a href="{ep}">link</a></body></html>'
        with patch(
            "application.blueprints.datamanager.services.doc_crawler.requests.get",
            return_value=_mock_response(html),
        ):
            with app.app_context():
                result = check_endpoint_in_doc(doc, ep)
        assert result["found"] is True
        assert result["matched_href"] == ep
        assert result["error"] is None

    def test_access_error_returns_error_string_orange(self, app):
        """doc gov.uk + HTTP error → Orange (access error)"""
        doc = "https://www.example.gov.uk/data/access-error/"
        ep = "https://maps.example.gov.uk/layer-access-error.geojson"
        exc = requests_lib.exceptions.RequestException("403 Forbidden")
        with patch(
            "application.blueprints.datamanager.services.doc_crawler.requests.get",
            side_effect=exc,
        ):
            with app.app_context():
                result = check_endpoint_in_doc(doc, ep)
        assert result["found"] is False
        assert result["error"] is not None
        assert "403" in result["error"]

    def test_timeout_returns_error_string_orange(self, app):
        """doc gov.uk + timeout → Orange (access error)"""
        doc = "https://www.example.gov.uk/data/timeout/"
        ep = "https://maps.example.gov.uk/layer-timeout.geojson"
        with patch(
            "application.blueprints.datamanager.services.doc_crawler.requests.get",
            side_effect=requests_lib.exceptions.Timeout(),
        ):
            with app.app_context():
                result = check_endpoint_in_doc(doc, ep)
        assert result["found"] is False
        assert result["error"] is not None

    def test_endpoint_not_found_gov_uk_endpoint_orange(self, app):
        """doc gov.uk + not found + endpoint gov.uk → Orange"""
        doc = "https://www.example.gov.uk/data/not-found-govuk/"
        ep = "https://maps.example.gov.uk/layer-govuk-notfound.geojson"
        html = "<html><body><a href='https://other.example.gov.uk/unrelated'>x</a></body></html>"
        with patch(
            "application.blueprints.datamanager.services.doc_crawler.requests.get",
            return_value=_mock_response(html),
        ):
            with app.app_context():
                result = check_endpoint_in_doc(doc, ep)
        assert result["found"] is False
        assert result["error"] is None
        # Caller checks is_gov_uk_url(endpoint_url) to determine Orange vs Red
        assert is_gov_uk_url(ep) is True

    def test_endpoint_not_found_non_gov_uk_endpoint_red(self, app):
        """doc gov.uk + not found + non-gov.uk endpoint → Red"""
        doc = "https://www.example.gov.uk/data/not-found-nongov/"
        ep = "https://services.arcgis.com/data/layer-nongov.geojson"
        html = "<html><body><a href='https://other.example.gov.uk/unrelated'>x</a></body></html>"
        with patch(
            "application.blueprints.datamanager.services.doc_crawler.requests.get",
            return_value=_mock_response(html),
        ):
            with app.app_context():
                result = check_endpoint_in_doc(doc, ep)
        assert result["found"] is False
        assert result["error"] is None
        assert is_gov_uk_url(ep) is False

    def test_non_gov_uk_doc_url_endpoint_found_still_red(self, app):
        """non-gov.uk doc + found → Red (caller checks is_gov_uk_url(doc_url))"""
        doc = "https://www.council.org.uk/data/found/"
        ep = "https://maps.example.gov.uk/layer-nongov-doc-found.geojson"
        html = f'<html><body><a href="{ep}">link</a></body></html>'
        with patch(
            "application.blueprints.datamanager.services.doc_crawler.requests.get",
            return_value=_mock_response(html),
        ):
            with app.app_context():
                result = check_endpoint_in_doc(doc, ep)
        assert result["found"] is True
        # Caller checks is_gov_uk_url(doc_url) to determine the Red override
        assert is_gov_uk_url(doc) is False

    def test_non_gov_uk_doc_url_endpoint_not_found_red(self, app):
        """non-gov.uk doc + not found → Red"""
        doc = "https://www.council.org.uk/data/not-found/"
        ep = "https://maps.example.gov.uk/layer-nongov-doc-notfound.geojson"
        html = "<html><body><a href='https://unrelated.org.uk/page'>x</a></body></html>"
        with patch(
            "application.blueprints.datamanager.services.doc_crawler.requests.get",
            return_value=_mock_response(html),
        ):
            with app.app_context():
                result = check_endpoint_in_doc(doc, ep)
        assert result["found"] is False
        assert result["error"] is None
        assert is_gov_uk_url(doc) is False
