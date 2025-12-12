import json
import responses
from unittest.mock import patch, Mock


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
                        "collection": "brownfield-land-collection",
                    }
                ]
            },
            status=200,
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
                    {
                        "name": "test-dataset",
                        "dataset": "test-id",
                        "collection": "test-collection",
                    }
                ]
            },
            status=200,
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
                            "value": "local-authority-eng:TEST",
                        }
                    }
                ]
            },
            status=200,
        )

        response = client.get("/datamanager/dashboard/add?get_orgs_for=test-dataset")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert "Test Council (TEST)" in data

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

        response = client.post("/datamanager/dashboard/add", data=form_data)
        assert response.status_code == 200

    @patch("application.blueprints.datamanager.views.requests.get")
    def test_boundary_url_entity_not_found(self, mock_get, client):
        """Test boundary URL generation when entity not found"""
        with patch(
            "application.blueprints.datamanager.views.get_request_api_endpoint"
        ) as mock_endpoint:
            mock_endpoint.return_value = "http://test-api"

            main_response = Mock()
            main_response.status_code = 200
            main_response.json.return_value = {
                "status": "COMPLETED",
                "response": {
                    "data": {
                        "entity-summary": {},
                        "new-entities": [],
                        "existing-entities": [],
                    }
                },
                "params": {"organisation": "local-authority-eng:ABC123"},
            }
            main_response.raise_for_status.return_value = None

            details_response = Mock()
            details_response.status_code = 200
            details_response.json.return_value = []
            details_response.raise_for_status.return_value = None

            # Mock empty entity response
            entity_response = Mock()
            entity_response.json.return_value = {"entities": []}
            entity_response.raise_for_status.return_value = None

            mock_get.side_effect = [main_response, details_response, entity_response]

            response = client.get("/datamanager/check-results/test-id")
            assert response.status_code == 200

    @patch("application.blueprints.datamanager.views.requests.get")
    def test_boundary_url_reference_not_found(self, mock_get, client):
        """Test boundary URL generation when reference not found"""
        with patch(
            "application.blueprints.datamanager.views.get_request_api_endpoint"
        ) as mock_endpoint:
            mock_endpoint.return_value = "http://test-api"

            main_response = Mock()
            main_response.status_code = 200
            main_response.json.return_value = {
                "status": "COMPLETED",
                "response": {
                    "data": {
                        "entity-summary": {},
                        "new-entities": [],
                        "existing-entities": [],
                    }
                },
                "params": {"organisation": "local-authority-eng:ABC123"},
            }
            main_response.raise_for_status.return_value = None

            details_response = Mock()
            details_response.status_code = 200
            details_response.json.return_value = []
            details_response.raise_for_status.return_value = None

            # Mock entity with no reference
            entity_response = Mock()
            entity_response.json.return_value = {
                "entities": [{"reference": None, "local-planning-authority": None}]
            }
            entity_response.raise_for_status.return_value = None

            mock_get.side_effect = [main_response, details_response, entity_response]

            response = client.get("/datamanager/check-results/test-id")
            assert response.status_code == 200

    @patch("application.blueprints.datamanager.views.get_spec_fields_union")
    @patch("application.blueprints.datamanager.views.read_raw_csv_preview")
    @patch("application.blueprints.datamanager.views.requests.post")
    @patch("application.blueprints.datamanager.views.requests.get")
    @patch("application.blueprints.datamanager.views.get_request_api_endpoint")
    def test_configure_post_mapping_logic_lines(
        self, mock_endpoint, mock_get, mock_post, mock_csv, mock_spec_fields, client
    ):
        """Test lines 895-962: Configure POST mapping logic and table building"""
        mock_endpoint.return_value = "http://test-api"
        mock_spec_fields.return_value = ["spec_field1", "required_field"]
        mock_csv.return_value = (["raw_field1", "raw_field2"], [["value1", "value2"]])

        # Mock initial request response
        req_response = Mock()
        req_response.status_code = 200
        req_response.json.return_value = {
            "params": {
                "dataset": "test-dataset",
                "url": "https://example.com/data.csv",
                "collection": "test-collection",
                "organisation": "test-org",
            },
            "response": {
                "data": {
                    "column-field-log": [{"field": "required_field", "missing": True}],
                    "column-mapping": {"existing_raw": "existing_spec"},
                }
            },
            "status": "COMPLETED",
        }

        # Mock successful POST response
        post_response = Mock()
        post_response.status_code = 202
        post_response.json.return_value = {"id": "new-request-id"}

        mock_get.return_value = req_response
        mock_post.return_value = post_response

        form_data = {
            "map_raw[raw_field1]": "spec_field1",
            "map_raw[raw_field2]": "__NOT_MAPPED__",
            "map_spec_to_spec[required_field]": "spec_field1",
            "geom_type": "point",
        }

        response = client.post("/datamanager/configure/test-id", data=form_data)
        assert response.status_code == 302

        # Verify POST was called with correct mapping
        mock_post.assert_called_once()
        call_args = mock_post.call_args[1]["json"]
        assert call_args["params"]["column_mapping"] == {"raw_field1": "required_field"}
        assert call_args["params"]["geom_type"] == "point"
