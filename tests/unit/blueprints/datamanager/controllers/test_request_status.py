from contextlib import ExitStack
from unittest.mock import patch

import pytest


@pytest.fixture(
    params=[
        ("/datamanager/check-results/test-id", "check"),
        ("/datamanager/add-data/test-id/entities", "preview"),
        ("/datamanager/check-transform/test-id", "transform"),
    ]
)
def status_page(request, client):
    url, controller = request.param
    module = f"application.blueprints.datamanager.controllers.{controller}"
    with ExitStack() as stack:
        stack.enter_context(
            patch(f"{module}.get_organisation_name", return_value="Test Org")
        )
        stack.enter_context(
            patch(f"{module}.get_dataset_name", return_value="Test Dataset")
        )
        yield client, url


def _get_status_page(status_page, status, response):
    client, url = status_page
    with patch(
        "application.blueprints.datamanager.router.fetch_request",
        return_value={
            "id": "test-id",
            "status": status,
            "response": response,
            "params": {
                "organisationName": "local-authority:ABC",
                "dataset": "brownfield-land",
            },
        },
    ):
        return client.get(url)


@pytest.mark.parametrize("status", ["NEW", "PROCESSING", "COMPLETE"])
def test_missing_response_shows_loading(status_page, status):
    response = _get_status_page(status_page, status, None)
    assert response.status_code == 200
    assert b"govuk-error-summary" not in response.data
    assert b'http-equiv="refresh"' in response.data


@pytest.mark.parametrize("status", ["UNKNOWN", "PENDING", "QUEUED", None])
@pytest.mark.parametrize("payload", [None, {"data": {}}])
def test_unknown_status_shows_error_without_refresh(status_page, status, payload):
    response = _get_status_page(status_page, status, payload)
    assert response.status_code == 200
    assert b"Unrecognised request status" in response.data
    assert b"govuk-error-summary" in response.data
    assert b'http-equiv="refresh"' not in response.data


def test_failure_without_response_shows_error_without_refresh(status_page):
    response = _get_status_page(status_page, "FAILED", None)
    assert response.status_code in (200, 404)
    assert b"govuk-error-summary" in response.data
    assert b'http-equiv="refresh"' not in response.data
