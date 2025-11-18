import json
import pytest
from unittest.mock import patch, Mock

from application.blueprints.datamanager.views import (
    get_spec_fields_union,
    read_raw_csv_preview,
)


class TestDatamanagerViews:
    """Unit tests for datamanager views"""

    def test_index_route(self, client):
        """Test the index route returns correct template"""
        response = client.get("/datamanager/")
        assert response.status_code == 200

    def test_dashboard_config_route(self, client):
        """Test the dashboard config route"""
        response = client.get("/datamanager/dashboard/config")
        assert response.status_code == 200

    @patch("application.blueprints.datamanager.views.requests.get")
    def test_dashboard_add_get(self, mock_get, client):
        """Test dashboard add GET request"""
        mock_response = Mock()
        mock_response.json.return_value = {
            "datasets": [
                {
                    "name": "test-dataset",
                    "dataset": "test-id",
                    "collection": "test-collection",
                }
            ]
        }
        mock_get.return_value = mock_response

        response = client.get("/datamanager/dashboard/add")
        assert response.status_code == 200

    @patch("application.blueprints.datamanager.views.requests.get")
    def test_dashboard_add_autocomplete(self, mock_get, client):
        """Test dashboard add autocomplete functionality"""
        mock_response = Mock()
        mock_response.json.return_value = {
            "datasets": [
                {
                    "name": "test-dataset",
                    "dataset": "test-id",
                    "collection": "test-collection",
                },
                {
                    "name": "another-dataset",
                    "dataset": "another-id",
                    "collection": "another-collection",
                },
            ]
        }
        mock_get.return_value = mock_response

        response = client.get("/datamanager/dashboard/add?autocomplete=test")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert "test-dataset" in data

    @patch("application.blueprints.datamanager.views.requests.get")
    def test_dashboard_add_get_orgs_for(self, mock_get, client):
        """Test getting organizations for a dataset"""
        # Mock dataset response
        dataset_response = Mock()
        dataset_response.json.return_value = {
            "datasets": [
                {
                    "name": "test-dataset",
                    "dataset": "test-id",
                    "collection": "test-collection",
                }
            ]
        }

        # Mock provision response
        provision_response = Mock()
        provision_response.json.return_value = {
            "rows": [{"organisation": {"label": "Test Org", "value": "prefix:TEST123"}}]
        }

        mock_get.side_effect = [dataset_response, provision_response]

        response = client.get("/datamanager/dashboard/add?get_orgs_for=test-dataset")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert "Test Org (TEST123)" in data


class TestUtilityFunctions:
    """Unit tests for utility functions"""

    @patch("application.blueprints.datamanager.views.requests.get")
    def test_get_spec_fields_union_success(self, mock_get):
        """Test successful field union retrieval"""
        mock_response = Mock()
        mock_response.json.return_value = [{"field": "field1"}, {"field": "field2"}]
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        result = get_spec_fields_union("test-dataset")
        assert "field1" in result
        assert "field2" in result

    @patch("application.blueprints.datamanager.views.requests.get")
    def test_get_spec_fields_union_no_dataset(self, mock_get):
        """Test field union retrieval without dataset ID"""
        mock_response = Mock()
        mock_response.json.return_value = [{"field": "global_field"}]
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        result = get_spec_fields_union(None)
        assert "global_field" in result
        assert mock_get.call_count == 1  # Only global call

    @patch("application.blueprints.datamanager.views.requests.get")
    def test_get_spec_fields_union_request_failure(self, mock_get):
        """Test field union retrieval with request failure"""
        mock_get.side_effect = Exception("Network error")

        result = get_spec_fields_union("test-dataset")
        assert result == []

    @patch("application.blueprints.datamanager.views.requests.get")
    def test_read_raw_csv_preview_success(self, mock_get):
        """Test successful CSV preview reading"""
        csv_content = "header1,header2\nvalue1,value2\nvalue3,value4"
        mock_response = Mock()
        mock_response.content = csv_content.encode("utf-8")
        mock_get.return_value = mock_response

        headers, rows = read_raw_csv_preview("http://example.com/test.csv")
        assert headers == ["header1", "header2"]
        assert len(rows) == 2
        assert rows[0] == ["value1", "value2"]

    def test_read_raw_csv_preview_empty_url(self):
        """Test CSV preview with empty URL"""
        headers, rows = read_raw_csv_preview("")
        assert headers == []
        assert rows == []

    @patch("application.blueprints.datamanager.views.requests.get")
    def test_read_raw_csv_preview_request_failure(self, mock_get):
        """Test CSV preview with request failure"""
        mock_get.side_effect = Exception("Network error")

        headers, rows = read_raw_csv_preview("http://example.com/test.csv")
        assert headers == []
        assert rows == []

    @patch("application.blueprints.datamanager.views.requests.get")
    def test_read_raw_csv_preview_with_bom(self, mock_get):
        """Test CSV preview with BOM character"""
        csv_content = "\ufeffheader1,header2\nvalue1,value2"
        mock_response = Mock()
        mock_response.content = csv_content.encode("utf-8")
        mock_get.return_value = mock_response

        headers, rows = read_raw_csv_preview("http://example.com/test.csv")
        assert headers == ["header1", "header2"]  # BOM should be stripped

    @patch("application.blueprints.datamanager.views.requests.get")
    def test_read_raw_csv_preview_max_rows(self, mock_get):
        """Test CSV preview respects max_rows parameter"""
        csv_content = "header1,header2\n" + "\n".join(
            [f"value{i},value{i+1}" for i in range(100)]
        )
        mock_response = Mock()
        mock_response.content = csv_content.encode("utf-8")
        mock_get.return_value = mock_response

        headers, rows = read_raw_csv_preview("http://example.com/test.csv", max_rows=5)
        assert len(rows) == 5
