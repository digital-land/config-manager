from unittest.mock import patch, Mock

import pytest

from application.blueprints.datamanager.services.async_api import (
    AsyncAPIError,
    fetch_request,
    submit_request,
)


class TestSubmitRequest:
    def test_returns_request_id_on_202(self):
        mock_response = Mock()
        mock_response.status_code = 202
        mock_response.json.return_value = {"id": "abc123"}
        with patch("application.blueprints.datamanager.services.async_api.requests.post", return_value=mock_response):
            result = submit_request({"type": "check_url", "url": "https://example.com/data.csv"})
        assert result == "abc123"

    def test_raises_on_non_202(self):
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.json.return_value = {"error": "server error"}
        with patch("application.blueprints.datamanager.services.async_api.requests.post", return_value=mock_response):
            with pytest.raises(AsyncAPIError):
                submit_request({"type": "check_url"})

    def test_raises_on_request_exception(self):
        with patch("application.blueprints.datamanager.services.async_api.requests.post", side_effect=Exception("timeout")):
            with pytest.raises(Exception):
                submit_request({"type": "check_url"})


class TestFetchRequest:
    def test_returns_json_on_200(self):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"id": "abc123", "status": "COMPLETE"}
        with patch("application.blueprints.datamanager.services.async_api.requests.get", return_value=mock_response):
            result = fetch_request("abc123")
        assert result["id"] == "abc123"
        assert result["status"] == "COMPLETE"

    def test_raises_on_404(self):
        mock_response = Mock()
        mock_response.status_code = 404
        with patch("application.blueprints.datamanager.services.async_api.requests.get", return_value=mock_response):
            with pytest.raises(AsyncAPIError):
                fetch_request("nonexistent-id")

    def test_raises_on_400(self):
        mock_response = Mock()
        mock_response.status_code = 400
        with patch("application.blueprints.datamanager.services.async_api.requests.get", return_value=mock_response):
            with pytest.raises(AsyncAPIError):
                fetch_request("bad-id")
