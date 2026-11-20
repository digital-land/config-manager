import time
from unittest.mock import Mock, patch

from application.data_access.http import (
    fetch_pages_concurrently,
    get_http_session,
)


def _response(payload):
    response = Mock(status_code=200)
    response.json.return_value = payload
    response.raise_for_status.return_value = None
    return response


class TestFetchPagesConcurrently:
    def test_returns_empty_for_no_urls(self):
        assert fetch_pages_concurrently([]) == []

    def test_preserves_input_order_regardless_of_completion_order(self):
        # Make the first url the slowest so append-as-completed ordering would
        # reverse the result.
        delays = {"/a": 0.05, "/b": 0.02, "/c": 0.0}

        def slow_get(url, **kwargs):
            time.sleep(delays[url])
            return _response({"page": url})

        session = Mock()
        session.get.side_effect = slow_get
        with patch(
            "application.data_access.http.get_http_session",
            return_value=session,
        ):
            result = fetch_pages_concurrently(["/a", "/b", "/c"])

        assert result == [{"page": "/a"}, {"page": "/b"}, {"page": "/c"}]

    def test_failed_page_yields_none_without_losing_the_others(self):
        def get(url, **kwargs):
            if url == "/b":
                raise Exception("boom")
            return _response({"page": url})

        session = Mock()
        session.get.side_effect = get
        with patch(
            "application.data_access.http.get_http_session",
            return_value=session,
        ):
            result = fetch_pages_concurrently(["/a", "/b", "/c"])

        assert result == [{"page": "/a"}, None, {"page": "/c"}]

    def test_non_200_becomes_none(self):
        response = Mock()
        response.raise_for_status.side_effect = Exception("500 Server Error")
        session = Mock()
        session.get.return_value = response
        with patch(
            "application.data_access.http.get_http_session",
            return_value=session,
        ):
            assert fetch_pages_concurrently(["/a"]) == [None]


class TestGetHttpSession:
    def test_is_reused_across_calls(self):
        assert get_http_session() is get_http_session()

    def test_pool_is_large_enough_for_the_worker_count(self):
        from application.data_access import http

        adapter = get_http_session().get_adapter("https://example.com")
        assert adapter._pool_maxsize >= http.DEFAULT_MAX_WORKERS
