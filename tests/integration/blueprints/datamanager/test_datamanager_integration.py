import json
import responses


class TestDatamanagerIntegration:
    """Integration tests for datamanager blueprint"""

    def test_index_renders_template(self, client):
        """Test index route renders correctly"""
        response = client.get("/datamanager/")
        assert response.status_code == 200
        assert b"Dashboard" in response.data

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
                        "collection": "brownfield-land-collection",
                    }
                ]
            },
            status=200,
        )

        response = client.get("/datamanager/add")
        assert response.status_code == 200

    @responses.activate
    def test_dashboard_add_autocomplete_integration(self, client):
        """Test autocomplete functionality with external API"""
        responses.add(
            responses.GET,
            "https://www.planning.data.gov.uk/dataset.json?_labels=on&_size=max",
            json={
                "datasets": [
                    {
                        "name": "brownfield-land",
                        "dataset": "brownfield-land",
                        "collection": "test",
                    },
                    {
                        "name": "conservation-area",
                        "dataset": "conservation-area",
                        "collection": "test",
                    },
                ]
            },
            status=200,
        )

        response = client.get("/datamanager/add?autocomplete=brown")
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
                    {
                        "name": "test-dataset",
                        "dataset": "test-id",
                        "collection": "test-collection",
                    }
                ]
            },
            status=200,
        )

        # Mock provision CSV from GitHub
        responses.add(
            responses.GET,
            "https://raw.githubusercontent.com/digital-land/specification/refs/heads/main/specification/provision.csv",
            body="dataset,organisation\ntest-id,local-authority-eng:TEST\n",
            status=200,
        )

        # Mock organisation.json API
        responses.add(
            responses.GET,
            "https://datasette.planning.data.gov.uk/digital-land/organisation.json?_shape=objects&_size=max",
            json={
                "rows": [
                    {
                        "organisation": "local-authority-eng:TEST",
                        "name": "Test Council",
                    }
                ]
            },
            status=200,
        )

        response = client.get("/datamanager/add?get_orgs_for=test-dataset")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert "Test Council (local-authority-eng:TEST)" in data

    @responses.activate
    def test_dashboard_add_post_integration(self, client):
        """Test POST request to dashboard add"""
        responses.add(
            responses.GET,
            "https://www.planning.data.gov.uk/dataset.json?_labels=on&_size=max",
            json={
                "datasets": [
                    {
                        "name": "test-dataset",
                        "dataset": "test-id",
                        "collection": "test-collection",
                    }
                ]
            },
            status=200,
        )

        form_data = {
            "mode": "preview",
            "dataset": "test-dataset",
            "endpoint_url": "http://example.com/data.csv",
        }

        response = client.post("/datamanager/add", data=form_data)
        assert response.status_code == 200
