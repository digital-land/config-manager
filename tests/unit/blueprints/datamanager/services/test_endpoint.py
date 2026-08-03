from unittest.mock import MagicMock, patch

from application.blueprints.datamanager.services.endpoint import (
    get_endpoint_log_summary_for_hashes,
    get_endpoint_info_for_hashes,
)

ENDPOINT_MODULE = "application.blueprints.datamanager.services.endpoint"


def _objects_response(rows):
    resp = MagicMock()
    resp.raise_for_status = MagicMock()
    resp.json.return_value = {"rows": rows}
    return resp


class TestGetEndpointUrlsForHashes:
    def test_empty_input_returns_empty_dict(self):
        assert get_endpoint_info_for_hashes([]) == {}

    def test_maps_url_entry_and_end_date(self, app):
        rows = [
            {
                "endpoint": "hash-a",
                "endpoint_url": "https://example.com/a.csv",
                "entry_date": "2026-01-01",
                "end_date": "",
            }
        ]
        with app.app_context():
            with patch(f"{ENDPOINT_MODULE}.requests.get") as mock_get:
                mock_get.return_value = _objects_response(rows)
                result = get_endpoint_info_for_hashes(["hash-a"])

        assert result == {
            "hash-a": {
                "endpoint_url": "https://example.com/a.csv",
                "entry_date": "2026-01-01",
                "end_date": "",
            }
        }
        called_url = mock_get.call_args[0][0]
        assert "endpoint.json" in called_url
        assert "endpoint__in=hash-a" in called_url

    def test_exception_returns_empty_dict(self, app):
        with app.app_context():
            with patch(
                f"{ENDPOINT_MODULE}.requests.get", side_effect=Exception("boom")
            ):
                assert get_endpoint_info_for_hashes(["hash-a"]) == {}


class TestGetEndpointLogSummaryForHashes:
    def test_empty_input_returns_empty_dict(self):
        assert get_endpoint_log_summary_for_hashes([]) == {}

    def test_maps_latest_row_per_endpoint(self, app):
        # Two rows for the same endpoint; the most recent log date wins.
        rows = [
            {
                "endpoint": "hash-a",
                "latest_status": "404",
                "latest_log_entry_date": "2026-07-10",
            },
            {
                "endpoint": "hash-a",
                "latest_status": "200",
                "latest_log_entry_date": "2026-07-20",
            },
        ]
        with app.app_context():
            with patch(f"{ENDPOINT_MODULE}.requests.get") as mock_get:
                mock_get.return_value = _objects_response(rows)
                result = get_endpoint_log_summary_for_hashes(["hash-a", "hash-b"])

        assert result == {
            "hash-a": {
                "latest_status": "200",
                "latest_log_entry_date": "2026-07-20",
            }
        }
        # A single precomputed-table lookup is issued for all hashes.
        assert mock_get.call_count == 1
        called_url = mock_get.call_args[0][0]
        assert "performance/reporting_historic_endpoints.json" in called_url
        assert "endpoint__in=hash-a,hash-b" in called_url

    def test_exception_returns_empty_dict(self, app):
        with app.app_context():
            with patch(
                f"{ENDPOINT_MODULE}.requests.get", side_effect=Exception("slow")
            ):
                assert get_endpoint_log_summary_for_hashes(["hash-a"]) == {}
