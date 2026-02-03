import responses
from unittest.mock import patch, Mock


class TestDatamanagerAcceptance:
    """Acceptance tests for datamanager blueprint end-to-end workflows"""

    @responses.activate
    def test_complete_data_submission_workflow(self, client):
        """Test complete workflow from dashboard to data submission"""
        # Mock external APIs
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

        responses.add(
            responses.GET,
            (
                "https://datasette.planning.data.gov.uk/digital-land/"
                "provision.json?_labels=on&_size=max&dataset=brownfield-land"
            ),
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

        # Mock GitHub provision CSV
        responses.add(
            responses.GET,
            (
                "https://raw.githubusercontent.com/digital-land/specification/"
                "refs/heads/main/specification/provision.csv"
            ),
            body=(
                "dataset,organisation-entity,provision-reason,provision-date,notes\n"
                "brownfield-land,local-authority-eng:TEST,expected,2024-01-01,"
                "Test provision"
            ),
            status=200,
        )

        # Mock organisation.json endpoint
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

        # Step 1: Access dashboard
        response = client.get("/datamanager/add")
        assert response.status_code == 200

        # Step 2: Submit form data
        with patch(
            "application.blueprints.datamanager.views.requests.post"
        ) as mock_post:
            mock_post.return_value.status_code = 202
            mock_post.return_value.json.return_value = {"id": "test-request-123"}

            form_data = {
                "mode": "final",
                "dataset": "brownfield-land",
                "organisation": "Test Council (local-authority-eng:TEST)",
                "endpoint_url": "https://example.com/data.csv",
                "documentation_url": "https://example.gov.uk/docs",
                "licence": "ogl",
                "start_day": "1",
                "start_month": "1",
                "start_year": "2024",
            }

            response = client.post("/datamanager/add", data=form_data)
            assert response.status_code == 302
            assert "/check-results/test-request-123" in response.location

    def test_entities_preview_workflow(self, client):
        """Test entities preview functionality"""
        with patch("application.blueprints.datamanager.views.requests.get") as mock_get:
            mock_get.return_value.status_code = 200
            mock_get.return_value.json.return_value = {
                "status": "COMPLETED",
                "response": {
                    "data": {
                        "pipeline-summary": {
                            "existing-in-resource": 3,
                            "new-in-resource": 2,
                            "new-entities": [
                                {
                                    "reference": "REF1",
                                    "prefix": "test",
                                    "organisation": "TEST",
                                    "entity": "123",
                                },
                                {
                                    "reference": "REF2",
                                    "prefix": "test",
                                    "organisation": "TEST",
                                    "entity": "124",
                                },
                            ],
                        },
                        "endpoint-summary": {},
                        "source-summary": {},
                    }
                },
                "params": {},
            }

            response = client.get("/datamanager/add-data/test-request-123/entities")
            assert response.status_code == 200

    def test_add_data_workflow(self, client):
        """Test add data workflow with session management"""
        with client.session_transaction() as sess:
            sess["required_fields"] = {
                "collection": "test-collection",
                "dataset": "test-dataset",
                "url": "https://example.com/data.csv",
                "organisation": "local-authority-eng:TEST",
            }
            sess["optional_fields"] = {
                "documentation_url": "https://example.gov.uk/docs",
                "licence": "ogl",
                "start_date": "2024-01-01",
            }

        with patch(
            "application.blueprints.datamanager.views.requests.post"
        ) as mock_post:
            mock_post.return_value.status_code = 202
            mock_post.return_value.json.return_value = {"id": "preview-123"}

            response = client.get("/datamanager/add-data")
            assert response.status_code == 302

    def test_add_data_confirm_workflow(self, client):
        """Test add data confirmation workflow"""
        with patch(
            "application.blueprints.datamanager.views.trigger_add_data_workflow"
        ) as mock_workflow, patch(
            "application.blueprints.datamanager.views.requests.get"
        ) as mock_get:

            mock_get.return_value.status_code = 200
            mock_get.return_value.json.return_value = {
                "params": {
                    "type": "add_data",
                    "dataset": "test-dataset",
                },
                "response": {
                    "data": {
                        "pipeline-summary": {"new-entities": []},
                        "endpoint-summary": {},
                        "source-summary": {
                            "new_source_entry": {"collection": "test-collection"}
                        },
                    }
                },
            }

            mock_workflow.return_value = {
                "success": True,
                "message": "Workflow triggered successfully",
            }

            with client.session_transaction() as sess:
                sess["user"] = {"login": "test-user"}

            response = client.post("/datamanager/add-data/preview-123/confirm")
            assert response.status_code == 200

    def test_form_validation_workflow(self, client):
        """Test form validation in submission workflow"""
        with responses.RequestsMock() as rsps:
            rsps.add(
                responses.GET,
                "https://www.planning.data.gov.uk/dataset.json?_labels=on&_size=max",
                json={
                    "datasets": [
                        {"name": "test", "dataset": "test", "collection": "test"}
                    ]
                },
                status=200,
            )

            # Submit invalid form data
            form_data = {
                "mode": "final",
                "dataset": "",  # Missing required field
                "endpoint_url": "invalid-url",  # Invalid URL
            }

            response = client.post("/datamanager/add", data=form_data)
            assert response.status_code == 200  # Returns form with errors

    def test_loading_states_workflow(self, client):
        """Test loading states during processing"""
        with patch("application.blueprints.datamanager.views.requests.get") as mock_get:
            # Test pending status
            mock_get.return_value.status_code = 200
            mock_get.return_value.json.return_value = {
                "status": "PENDING",
                "response": None,
            }

            response = client.get("/datamanager/check-results/test-request-123")
            assert response.status_code == 200
            assert b"loading" in response.data.lower()

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

    @patch("application.blueprints.datamanager.utils.get_spec_fields_union")
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
            "map[raw_field1]": "required_field",
            "map[raw_field2]": "__NOT_MAPPED__",
            "geom_type": "point",
        }

        response = client.post(
            "/datamanager/check-results/test-id/configure-columns", data=form_data
        )
        assert response.status_code == 302

        # Verify POST was called with correct mapping
        mock_post.assert_called_once()
        call_args = mock_post.call_args[1]["json"]
        assert call_args["params"]["column_mapping"] == {"raw_field1": "required_field"}
        assert call_args["params"]["geom_type"] == "point"
