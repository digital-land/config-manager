import pytest

from application.blueprints.datamanager.services import async_api


@pytest.mark.parametrize("status", ["NEW", "PROCESSING"])
@pytest.mark.parametrize("response", [None, {}, {"data": {}}])
def test_active_requests_are_processing(status, response):
    assert async_api.is_processing({"status": status, "response": response})


def test_complete_request_without_response_is_not_ready():
    assert async_api.is_processing({"status": "COMPLETE"})
    assert async_api.is_processing({"status": "COMPLETE", "response": None})


@pytest.mark.parametrize("response", [{}, {"data": {}}])
def test_complete_request_with_response_is_ready(response):
    assert not async_api.is_processing({"status": "COMPLETE", "response": response})


@pytest.mark.parametrize("response", [None, {}, {"error": None}])
def test_failed_request_raises_even_without_error_details(response):
    with pytest.raises(async_api.AsyncAPIError, match="Async processing failed"):
        async_api.is_processing({"status": "FAILED", "response": response})


def test_failed_request_preserves_error_message():
    with pytest.raises(async_api.AsyncAPIError, match="Unable to read resource"):
        async_api.is_processing(
            {
                "status": "FAILED",
                "response": {"error": {"errMsg": "Unable to read resource"}},
            }
        )


@pytest.mark.parametrize("status", [None, "", "UNKNOWN", "PENDING", "QUEUED"])
@pytest.mark.parametrize("response", [None, {"data": {}}])
def test_unknown_status_raises_instead_of_loading_or_showing_results(status, response):
    with pytest.raises(async_api.AsyncAPIError, match="Unrecognised request status"):
        async_api.is_processing({"status": status, "response": response})
