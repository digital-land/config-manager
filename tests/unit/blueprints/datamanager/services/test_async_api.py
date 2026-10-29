from unittest.mock import patch, Mock

import pytest
import jwt
from flask import Flask, session
from werkzeug.exceptions import Unauthorized

from application.blueprints.datamanager.utils import handle_error

from application.blueprints.datamanager.services.async_api import (
    fetch_response_details,
    AsyncAPIError,
    ResponseDetailsIncomplete,
    fetch_request,
    submit_request,
)


class TestSubmitRequest:
    @pytest.mark.parametrize("is_admin,role", [(False, "staff"), (True, "admin")])
    def test_session_can_issue_another_token_after_previous_expiry(
        self, app, is_admin, role
    ):
        from application.blueprints.auth.auth_token import get_auth_token

        with app.test_request_context():
            session["user"] = {"id": 123, "login": "alice", "is_admin": is_admin}
            with patch(
                "application.blueprints.auth.auth_token.time.time", return_value=1000
            ):
                first = get_auth_token()
            with patch(
                "application.blueprints.auth.auth_token.time.time", return_value=2000
            ):
                second = get_auth_token()
            first_claims = jwt.decode(first, options={"verify_signature": False})
            second_claims = jwt.decode(second, options={"verify_signature": False})
            assert first_claims["exp"] < second_claims["iat"]
            assert second_claims["role"] == role
            assert "auth_token" not in session
            session.pop("user")
            assert get_auth_token() is None

    def test_add_data_issues_fresh_token_from_github_session(self, app):
        response = Mock(status_code=202)
        response.json.return_value = {"id": "abc123"}
        with app.test_request_context(), patch(
            "application.blueprints.datamanager.services.async_api.requests.post",
            return_value=response,
        ) as post:
            session["user"] = {"id": 123, "login": "alice", "is_admin": False}
            session["auth_token"] = "expired-token"
            assert submit_request({"type": "add_data"}) == "abc123"
            token = post.call_args.kwargs["headers"]["Authorization"].removeprefix(
                "Bearer "
            )
            claims = jwt.decode(
                token,
                app.config["AUTH_JWT_PRIVATE_KEY"].public_key(),
                algorithms=["RS256"],
                audience=app.config["AUTH_JWT_AUDIENCE"],
                issuer=app.config["AUTH_JWT_ISSUER"],
            )
            assert claims["sub"] == "123"
            assert claims["username"] == "alice"
            assert claims["role"] == "staff"
            assert claims["exp"] - claims["iat"] == 900
            assert session["auth_token"] == "expired-token"

    @pytest.mark.parametrize("expires", [0, 1])
    def test_add_data_without_current_token_does_not_send(self, app, expires):
        with app.test_request_context(), patch(
            "application.blueprints.datamanager.services.async_api.requests.post"
        ) as post:
            session["auth_token_expires"] = expires
            if expires:
                session["auth_token"] = "expired-token"
            with pytest.raises(Unauthorized) as error:
                submit_request({"type": "add_data"})
        assert error.value.code == 401
        post.assert_not_called()

    def test_missing_session_user_returns_http_401(self):
        app = Flask(__name__)
        app.config["SECRET_KEY"] = "test"
        app.register_error_handler(Exception, handle_error)

        @app.post("/add-data")
        def add_data():
            return submit_request({"type": "add_data"})

        with patch(
            "application.blueprints.datamanager.services.async_api.requests.post"
        ) as post:
            response = app.test_client().post("/add-data")
        assert response.status_code == 401
        post.assert_not_called()

    def test_returns_request_id_on_202(self):
        mock_response = Mock()
        mock_response.status_code = 202
        mock_response.json.return_value = {"id": "abc123"}
        with patch(
            "application.blueprints.datamanager.services.async_api.requests.post",
            return_value=mock_response,
        ) as mock_post:
            result = submit_request(
                {"type": "check_url", "url": "https://example.com/data.csv"}
            )
        assert result == "abc123"
        assert mock_post.call_args.kwargs["json"]["params"]["service"] == "manage"

    def test_raises_on_non_202(self):
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.json.return_value = {"error": "server error"}
        with patch(
            "application.blueprints.datamanager.services.async_api.requests.post",
            return_value=mock_response,
        ):
            with pytest.raises(AsyncAPIError):
                submit_request({"type": "check_url"})

    def test_raises_on_request_exception(self):
        with patch(
            "application.blueprints.datamanager.services.async_api.requests.post",
            side_effect=Exception("timeout"),
        ):
            with pytest.raises(Exception):
                submit_request({"type": "check_url"})


class TestFetchRequest:
    def test_returns_json_on_200(self):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"id": "abc123", "status": "COMPLETE"}
        with patch(
            "application.blueprints.datamanager.services.async_api.requests.get",
            return_value=mock_response,
        ):
            result = fetch_request("abc123")
        assert result["id"] == "abc123"
        assert result["status"] == "COMPLETE"

    def test_raises_on_404(self):
        mock_response = Mock()
        mock_response.status_code = 404
        with patch(
            "application.blueprints.datamanager.services.async_api.requests.get",
            return_value=mock_response,
        ):
            with pytest.raises(AsyncAPIError):
                fetch_request("nonexistent-id")

    def test_raises_on_400(self):
        mock_response = Mock()
        mock_response.status_code = 400
        with patch(
            "application.blueprints.datamanager.services.async_api.requests.get",
            return_value=mock_response,
        ):
            with pytest.raises(AsyncAPIError):
                fetch_request("bad-id")


def _rows(start, count):
    """Identifiable rows so ordering can be asserted."""
    return [
        {"entry_number": n, "converted_row": {}} for n in range(start, start + count)
    ]


def _page_response(payload, total=None):
    response = Mock()
    response.json.return_value = payload
    response.raise_for_status.return_value = None
    response.headers = (
        {} if total is None else {"X-Pagination-Total-Results": str(total)}
    )
    return response


class TestFetchResponseDetails:
    """
    The first page carries the row total in a header, so the remaining pages are
    fetched in parallel. Row order matters: the controller slices the returned list
    by index to paginate the transform tables.
    """

    def _patch_first_page(self, response):
        session = Mock()
        session.get.return_value = response
        return patch(
            "application.blueprints.datamanager.services.async_api.get_http_session",
            return_value=session,
        )

    def test_fetches_remaining_pages_in_parallel_and_keeps_offset_order(self, app):
        with self._patch_first_page(_page_response(_rows(0, 500), total=1200)), patch(
            "application.blueprints.datamanager.services.async_api.fetch_pages_concurrently",
            return_value=[_rows(500, 500), _rows(1000, 200)],
        ) as fetch_pages:
            result, total = fetch_response_details("parallel-order")

        assert len(result) == 1200
        assert [row["entry_number"] for row in result] == list(range(1200))
        urls = fetch_pages.call_args[0][0]
        assert [u.split("offset=")[1].split("&")[0] for u in urls] == ["500", "1000"]

    def test_single_page_needs_no_parallel_fetch(self, app):
        with self._patch_first_page(_page_response(_rows(0, 12), total=12)), patch(
            "application.blueprints.datamanager.services.async_api.fetch_pages_concurrently"
        ) as fetch_pages:
            result, total = fetch_response_details("single-page")

        assert len(result) == 12
        fetch_pages.assert_not_called()

    def test_max_rows_clamps_the_result(self, app):
        with self._patch_first_page(_page_response(_rows(0, 500), total=5000)), patch(
            "application.blueprints.datamanager.services.async_api.fetch_pages_concurrently",
            return_value=[_rows(500, 500)],
        ):
            result, total = fetch_response_details("clamped", max_rows=750)

        assert len(result) == 750
        assert [row["entry_number"] for row in result] == list(range(750))

    def test_falls_back_to_sequential_when_header_missing(self, app):
        with self._patch_first_page(_page_response(_rows(0, 500))), patch(
            "application.blueprints.datamanager.services.async_api._fetch_response_details_sequentially",
            return_value=(_rows(0, 900), 900),
        ) as sequential:
            result, total = fetch_response_details("no-header")

        assert len(result) == 900
        assert total == 900
        sequential.assert_called_once()

    def test_failed_page_raises_and_carries_the_rows_it_did_get(self, app):
        with self._patch_first_page(_page_response(_rows(0, 500), total=1500)), patch(
            "application.blueprints.datamanager.services.async_api.fetch_pages_concurrently",
            return_value=[None, _rows(1000, 500)],
        ):
            with pytest.raises(ResponseDetailsIncomplete) as excinfo:
                fetch_response_details("holed")

        # The partial rows keep offset order so the controller can still show them.
        assert [row["entry_number"] for row in excinfo.value.partial] == (
            list(range(500)) + list(range(1000, 1500))
        )

    def test_partial_result_is_not_cached_so_the_next_call_retries(self, app):
        first_page = _page_response(_rows(0, 500), total=1000)
        with self._patch_first_page(first_page), patch(
            "application.blueprints.datamanager.services.async_api.fetch_pages_concurrently",
            return_value=[None],
        ):
            with pytest.raises(ResponseDetailsIncomplete):
                fetch_response_details("flaky-request")

        # Same memoize key: the failure must not have poisoned the cache.
        with self._patch_first_page(first_page), patch(
            "application.blueprints.datamanager.services.async_api.fetch_pages_concurrently",
            return_value=[_rows(500, 500)],
        ):
            result, total = fetch_response_details("flaky-request")

        assert [row["entry_number"] for row in result] == list(range(1000))

    def test_first_page_failure_raises_rather_than_caching_an_empty_result(self, app):
        # Returning [] would be memoized for an hour and render as "no rows".
        session = Mock()
        session.get.side_effect = Exception("connection reset")
        with patch(
            "application.blueprints.datamanager.services.async_api.get_http_session",
            return_value=session,
        ):
            with pytest.raises(ResponseDetailsIncomplete):
                fetch_response_details("first-page-fails")

    def test_short_first_page_without_header_needs_no_sequential_refetch(self, app):
        # A page shorter than the limit is the last page, so don't page again.
        with self._patch_first_page(_page_response(_rows(0, 10))), patch(
            "application.blueprints.datamanager.services.async_api.requests.get"
        ) as sequential_get:
            result, total = fetch_response_details("short-first-page")

        assert len(result) == 10
        assert total == 10
        sequential_get.assert_not_called()

    def test_transport_failure_during_sequential_fallback_raises_with_partial(
        self, app
    ):
        # A full headerless first page sends us down the sequential path. The
        # transport then fails before any response is bound, which used to raise
        # UnboundLocalError out of the error handler itself.
        with self._patch_first_page(_page_response(_rows(0, 500))), patch(
            "application.blueprints.datamanager.services.async_api.requests.get",
            side_effect=Exception("connection reset"),
        ):
            with pytest.raises(ResponseDetailsIncomplete) as excinfo:
                fetch_response_details("sequential-transport-failure")

        # The seeded first page is carried through rather than refetched or lost.
        assert len(excinfo.value.partial) == 500

    def test_sequential_fallback_does_not_refetch_the_first_page(self, app):
        second_page = Mock()
        second_page.json.return_value = _rows(500, 10)
        second_page.raise_for_status.return_value = None
        second_page.status_code = 200
        second_page.content = b"[]"
        second_page.text = "[]"

        with self._patch_first_page(_page_response(_rows(0, 500))), patch(
            "application.blueprints.datamanager.services.async_api.requests.get",
            return_value=second_page,
        ) as sequential_get:
            result, total = fetch_response_details("seeded-sequential")

        assert [row["entry_number"] for row in result] == list(range(510))
        assert total == 510
        # One call only: the seeded first page is not requested a second time.
        assert sequential_get.call_count == 1
        assert sequential_get.call_args.kwargs["params"]["offset"] == 500


@pytest.mark.parametrize("expected_count", [2, 3])
def test_fetch_platform_comparison_checks_summary_count(app, expected_count):
    from application.blueprints.datamanager.services.async_api import (
        fetch_platform_comparison,
    )

    rows = [
        {"source": "resource", "entry_number": "1"},
        {"source": "platform", "entity": "999"},
    ]
    with app.app_context(), patch(
        "application.blueprints.datamanager.services.async_api.fetch_response_details",
        return_value=(rows, 2),
    ) as fetch:
        if expected_count == 2:
            assert fetch_platform_comparison("test-comparison", expected_count) == rows
        else:
            with pytest.raises(ResponseDetailsIncomplete):
                fetch_platform_comparison("test-comparison", expected_count)
    fetch.assert_called_once_with("test-comparison", comparison=True)


@pytest.mark.parametrize("has_total_header", [False, True])
def test_comparison_pagination_keeps_its_endpoint(app, has_total_header):
    first_rows = [{"entity": "1"}, {"entity": "2"}]
    second_rows = [{"entity": "3"}]
    first = Mock(status_code=200, content=b"[]")
    first.json.return_value = first_rows
    first.headers = {"X-Pagination-Total-Results": "3"} if has_total_header else {}
    second = Mock(status_code=200, content=b"[]")
    second.json.return_value = second_rows
    second.headers = {}
    request_id = f"comparison-pages-{has_total_header}"
    with app.app_context(), patch(
        "application.blueprints.datamanager.services.async_api.get_http_session"
    ) as http, patch(
        "application.blueprints.datamanager.services.async_api.fetch_pages_concurrently",
        return_value=[second_rows],
    ) as parallel, patch(
        "application.blueprints.datamanager.services.async_api.requests.get",
        return_value=second,
    ) as sequential:
        http.return_value.get.return_value = first
        rows, total = fetch_response_details(request_id, limit=2, comparison=True)
    assert rows == first_rows + second_rows
    assert total == 3
    assert http.return_value.get.call_args.args[0].endswith(
        f"/{request_id}/platform-comparison"
    )
    if has_total_header:
        assert parallel.call_args.args[0][0].endswith(
            f"/{request_id}/platform-comparison?offset=2&limit=2"
        )
        sequential.assert_not_called()
    else:
        assert sequential.call_args.args[0].endswith(
            f"/{request_id}/platform-comparison"
        )
        parallel.assert_not_called()
