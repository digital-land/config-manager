import responses
from unittest.mock import patch


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

        # Step 1: Access dashboard
        response = client.get("/datamanager/dashboard/add")
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
                "organisation": "Test Council (TEST)",
                "endpoint_url": "https://example.com/data.csv",
                "documentation_url": "https://example.gov.uk/docs",
                "licence": "ogl",
                "start_day": "1",
                "start_month": "1",
                "start_year": "2024",
            }

            response = client.post("/datamanager/dashboard/add", data=form_data)
            assert response.status_code == 302
            assert "/check-results/test-request-123" in response.location

    # @responses.activate
    # def test_check_results_workflow(self, client):
    #     """Test check results viewing workflow"""
    #     # Mock request API response
    #     with patch("application.blueprints.datamanager.views.requests.get") as mock_get:
    #         mock_get.return_value.status_code = 200
    #         mock_get.return_value.json.return_value = {
    #             "status": "COMPLETED",
    #             "params": {"organisation": "Test Council"},
    #             "response": {
    #                 "data": {
    #                     "row-count": 10,
    #                     "entity-summary": {
    #                         "existing-in-resource": 5,
    #                         "new-in-resource": 5,
    #                     },
    #                     "new-entities": [{"reference": "REF1", "entity": "123"}],
    #                     "error-summary": [],
    #                     "column-field-log": [],
    #                 }
    #             },
    #         }

    #         response = client.get("/datamanager/check-results/test-request-123")
    #         assert response.status_code == 200
    #         assert b"5 existing; 5 new" in response.data

    def test_entities_preview_workflow(self, client):
        """Test entities preview functionality"""
        with patch("application.blueprints.datamanager.views.requests.get") as mock_get:
            mock_get.return_value.status_code = 200
            mock_get.return_value.json.return_value = {
                "status": "COMPLETED",
                "response": {
                    "data": {
                        "entity-summary": {
                            "existing-in-resource": 3,
                            "new-in-resource": 2,
                        },
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
                        "endpoint_url_validation": {"found_in_endpoint_csv": True},
                    }
                },
            }

            response = client.get(
                "/datamanager/check-results/test-request-123/entities"
            )
            assert response.status_code == 200

    # @responses.activate
    # def test_configure_workflow(self, client):
    #     """Test configuration workflow with column mapping"""
    #     # Mock CSV data
    #     responses.add(
    #         responses.GET,
    #         "https://example.com/data.csv",
    #         body="Reference,Name,Status\nREF1,Test Site,Active\nREF2,Another Site,Inactive",
    #         status=200,
    #     )

    #     with patch("application.blueprints.datamanager.views.requests.get") as mock_get:
    #         mock_get.return_value.status_code = 200
    #         mock_get.return_value.json.return_value = {
    #             "params": {
    #                 "dataset": "brownfield-land",
    #                 "url": "https://example.com/data.csv",
    #                 "organisation": "local-authority-eng:TEST",
    #             },
    #             "response": {
    #                 "data": {
    #                     "column-field-log": [{"field": "reference", "missing": True}]
    #                 }
    #             },
    #         }

    #         response = client.get("/datamanager/configure/test-request-123")
    #         assert response.status_code == 200

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

            response = client.get("/datamanager/check-results/add-data")
            assert response.status_code == 302

    def test_add_data_confirm_workflow(self, client):
        """Test add data confirmation workflow"""
        with patch(
            "application.blueprints.datamanager.views.requests.get"
        ) as mock_get, patch(
            "application.blueprints.datamanager.views.requests.post"
        ) as mock_post:

            mock_get.return_value.status_code = 200
            mock_get.return_value.json.return_value = {
                "params": {
                    "type": "add_data",
                    "collection": "test-collection",
                    "dataset": "test-dataset",
                }
            }

            mock_post.return_value.status_code = 202
            mock_post.return_value.json.return_value = {
                "id": "final-123",
                "message": "Processing started",
            }

            response = client.post(
                "/datamanager/check-results/preview-123/add-data/confirm"
            )
            assert response.status_code == 302
            assert "/add-data/progress/final-123" in response.location

    # def test_add_data_result_workflow(self, client):
    #     """Test add data result viewing"""
    #     with patch("application.blueprints.datamanager.views.requests.get") as mock_get:
    #         mock_get.return_value.status_code = 200
    #         mock_get.return_value.json.return_value = {
    #             "status": "COMPLETED",
    #             "response": {
    #                 "data": {
    #                     "new-entity-count": 5,
    #                     "min-entity-after": "100",
    #                     "max-entity-after": "104",
    #                     "lookup-path": "/path/to/lookup.csv",
    #                     "new-entities": [
    #                         {
    #                             "reference": "REF1",
    #                             "prefix": "test",
    #                             "organisation": "TEST",
    #                             "entity": "100",
    #                         }
    #                     ],
    #                 }
    #             },
    #         }

    #         response = client.get("/datamanager/add-data/result/final-123")
    #         assert response.status_code == 200
    #         assert b"5" in response.data

    # @responses.activate
    # def test_error_handling_workflow(self, client):
    #     """Test error handling in various scenarios"""
    #     # Test API failure
    #     responses.add(
    #         responses.GET,
    #         "https://www.planning.data.gov.uk/dataset.json?_labels=on&_size=max",
    #         json={"error": "Service unavailable"},
    #         status=500,
    #     )

    #     response = client.get("/datamanager/dashboard/add")
    #     assert response.status_code == 500

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

            response = client.post("/datamanager/dashboard/add", data=form_data)
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

    # def test_pagination_workflow(self, client):
    #     """Test pagination in results viewing"""
    #     with patch("application.blueprints.datamanager.views.requests.get") as mock_get:
    #         mock_get.side_effect = [
    #             # First call for summary
    #             type(
    #                 "MockResponse",
    #                 (),
    #                 {
    #                     "status_code": 200,
    #                     "json": lambda: {
    #                         "status": "COMPLETED",
    #                         "params": {"organisation": "Test"},
    #                         "response": {"data": {"row-count": 100}},
    #                     },
    #                 },
    #             )(),
    #             # Second call for details
    #             type(
    #                 "MockResponse",
    #                 (),
    #                 {
    #                     "status_code": 200,
    #                     "json": lambda: [{"converted_row": {"field1": "value1"}}],
    #                     "raise_for_status": lambda: None,
    #                 },
    #             )(),
    #         ]

    #         response = client.get("/datamanager/check-results/test-request-123?page=2")
    #         assert response.status_code == 200
