import json
import pytest
import responses
from unittest.mock import patch


class TestDatamanagerIntegration:
    """Integration tests for datamanager blueprint"""

    def test_index_renders_template(self, client):
        """Test index route renders correctly"""
        response = client.get("/datamanager/")
        assert response.status_code == 200
        assert b"Dashboard" in response.data

    def test_dashboard_config_renders_template(self, client):
        """Test dashboard config route renders correctly"""
        response = client.get("/datamanager/dashboard/config")
        assert response.status_code == 200

    @responses.activate
    def test_dashboard_add_with_external_api(self, client):
        """Test dashboard add route with mocked external API calls"""
        # Mock the planning data API response
        responses.add(
            responses.GET,
            "https://www.planning.data.gov.uk/dataset.json?_labels=on&_size=max",
            json={
                "datasets": [
                    {
                        "name": "brownfield-land",
                        "dataset": "brownfield-land",
                        "collection": "brownfield-land-collection"
                    }
                ]
            },
            status=200
        )

        response = client.get("/datamanager/dashboard/add")
        assert response.status_code == 200

    @responses.activate
    def test_dashboard_add_autocomplete_integration(self, client):
        """Test autocomplete functionality with external API"""
        responses.add(
            responses.GET,
            "https://www.planning.data.gov.uk/dataset.json?_labels=on&_size=max",
            json={
                "datasets": [
                    {"name": "brownfield-land", "dataset": "brownfield-land", "collection": "test"},
                    {"name": "conservation-area", "dataset": "conservation-area", "collection": "test"}
                ]
            },
            status=200
        )

        response = client.get("/datamanager/dashboard/add?autocomplete=brown")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert "brownfield-land" in data

    @responses.activate
    def test_dashboard_add_get_orgs_integration(self, client):
        """Test get organizations functionality with external APIs"""
        # Mock dataset API
        responses.add(
            responses.GET,
            "https://www.planning.data.gov.uk/dataset.json?_labels=on&_size=max",
            json={
                "datasets": [
                    {"name": "test-dataset", "dataset": "test-id", "collection": "test-collection"}
                ]
            },
            status=200
        )

        # Mock provision API
        responses.add(
            responses.GET,
            "https://datasette.planning.data.gov.uk/digital-land/provision.json?_labels=on&_size=max&dataset=test-id",
            json={
                "rows": [
                    {
                        "organisation": {
                            "label": "Test Council",
                            "value": "local-authority-eng:TEST"
                        }
                    }
                ]
            },
            status=200
        )

        response = client.get("/datamanager/dashboard/add?get_orgs_for=test-dataset")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert "Test Council (TEST)" in data

    @responses.activate
    def test_dashboard_add_api_failure_handling(self, client):
        """Test handling of external API failures"""
        responses.add(
            responses.GET,
            "https://www.planning.data.gov.uk/dataset.json?_labels=on&_size=max",
            json={"error": "Service unavailable"},
            status=500
        )

        response = client.get("/datamanager/dashboard/add")
        assert response.status_code == 500

    @responses.activate
    def test_dashboard_add_post_integration(self, client):
        """Test POST request to dashboard add"""
        responses.add(
            responses.GET,
            "https://www.planning.data.gov.uk/dataset.json?_labels=on&_size=max",
            json={
                "datasets": [
                    {"name": "test-dataset", "dataset": "test-id", "collection": "test-collection"}
                ]
            },
            status=200
        )

        form_data = {
            "mode": "preview",
            "dataset": "test-dataset",
            "endpoint_url": "http://example.com/data.csv"
        }

        response = client.post("/datamanager/dashboard/add", data=form_data)
        assert response.status_code == 200