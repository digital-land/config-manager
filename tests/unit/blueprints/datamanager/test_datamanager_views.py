from unittest.mock import patch, MagicMock
from application.blueprints.datamanager.views import (
    get_spec_fields_union,
    read_raw_csv_preview,
)


class TestDatamanagerViews:
    def test_index_renders_successfully(self, client):
        """Test datamanager index page renders"""
        response = client.get("/datamanager/")
        assert response.status_code == 200

    def test_dashboard_config_renders_successfully(self, client):
        """Test dashboard config page renders"""
        response = client.get("/datamanager/dashboard/config")
        assert response.status_code == 200

    @patch("requests.get")
    def test_dashboard_add_get_renders_form(self, mock_get, client):
        """Test dashboard add GET renders form with datasets"""
        mock_get.return_value.json.return_value = {
            "datasets": [
                {
                    "name": "test-dataset",
                    "dataset": "test-id",
                    "collection": "test-collection",
                }
            ]
        }

        response = client.get("/datamanager/dashboard/add")
        assert response.status_code == 200

    @patch("requests.post")
    @patch("requests.get")
    def test_dashboard_add_post_success_stores_session(
        self, mock_get, mock_post, client
    ):
        """Test successful form submission stores data in session"""
        mock_get.side_effect = [
            MagicMock(
                json=lambda: {
                    "datasets": [
                        {
                            "name": "test-dataset",
                            "dataset": "test-id",
                            "collection": "test-collection",
                        }
                    ]
                }
            ),
            MagicMock(
                json=lambda: {
                    "rows": [
                        {
                            "organisation": {
                                "label": "Test Org",
                                "value": "local-authority:TEST",
                            }
                        }
                    ]
                }
            ),
        ]
        mock_post.return_value.status_code = 202
        mock_post.return_value.json.return_value = {"id": "test-request-id"}

        response = client.post(
            "/datamanager/dashboard/add",
            data={
                "mode": "final",
                "dataset": "test-dataset",
                "organisation": "Test Org (TEST)",
                "endpoint_url": "https://example.com/data.csv",
            },
        )

        assert response.status_code == 302
        with client.session_transaction() as sess:
            assert "required_fields" in sess

    @patch("requests.get")
    def test_check_results_loading_state(self, mock_get, client):
        """Test check results shows loading when processing"""
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {"status": "PROCESSING"}

        response = client.get("/datamanager/check-results/test-id")
        assert response.status_code == 200

    @patch("requests.get")
    def test_add_data_get_with_session_data(self, mock_get, client):
        """Test add_data GET uses session form_data"""
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {"params": {}}

        with client.session_transaction() as sess:
            sess["form_data"] = {"documentation_url": "https://test.gov.uk"}

        response = client.get("/datamanager/check-results/add-data")
        assert response.status_code == 200

    @patch("requests.post")
    @patch("requests.get")
    def test_add_data_post_updates_session(self, mock_get, mock_post, client):
        """Test add_data POST updates session optional_fields"""
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {"params": {}}
        mock_post.return_value.status_code = 202
        mock_post.return_value.json.return_value = {"id": "preview-id"}

        response = client.post(
            "/datamanager/check-results/add-data",
            data={
                "documentation_url": "https://new.gov.uk",
                "licence": "ogl",
                "start_day": "1",
                "start_month": "1",
                "start_year": "2024",
            },
        )

        assert response.status_code == 302
        with client.session_transaction() as sess:
            assert sess["optional_fields"]["documentation_url"] == "https://new.gov.uk"

    @patch("requests.get")
    def test_entities_preview_renders(self, mock_get, client):
        """Test entities preview page renders"""
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {
            "status": "COMPLETED",
            "response": {"data": {"entity-summary": {}, "new-entities": []}},
        }

        response = client.get("/datamanager/check-results/test-id/entities")
        assert response.status_code == 200

    @patch("requests.post")
    def test_optional_fields_submit(self, mock_post, client):
        """Test optional fields submission"""
        mock_post.return_value.status_code = 200

        response = client.post(
            "/datamanager/check-results/optional-submit",
            data={"request_id": "test-id", "documentation_url": "https://test.gov.uk"},
        )
        assert response.status_code == 302

    @patch("requests.post")
    @patch("requests.get")
    def test_add_data_confirm(self, mock_get, mock_post, client):
        """Test add data confirmation"""
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {"params": {}}
        mock_post.return_value.status_code = 202
        mock_post.return_value.json.return_value = {"id": "new-id"}

        response = client.post("/datamanager/check-results/test-id/add-data/confirm")
        assert response.status_code == 302

    @patch("application.blueprints.datamanager.views.render_template")
    @patch("requests.get")
    def test_configure_get(self, mock_get, mock_render, client):
        """Test configure page GET"""
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {
            "params": {"dataset": "test", "url": "https://test.com"},
            "response": {"data": {}},
        }
        mock_render.return_value = "mocked template"

        response = client.get("/datamanager/configure/test-id")
        assert response.status_code == 200

    @patch("application.blueprints.datamanager.views.render_template")
    def test_add_data_progress(self, mock_render, client):
        """Test add data progress page"""
        mock_render.return_value = "mocked template"
        response = client.get("/datamanager/add-data/progress/test-id")
        assert response.status_code == 200

    @patch("application.blueprints.datamanager.views.render_template")
    @patch("requests.get")
    def test_add_data_result(self, mock_get, mock_render, client):
        """Test add data result page"""
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {
            "status": "COMPLETED",
            "response": {"data": {"new-entities": []}},
        }
        mock_render.return_value = "mocked template"

        response = client.get("/datamanager/add-data/result/test-id")
        assert response.status_code == 200

    @patch("requests.get")
    def test_dashboard_add_autocomplete(self, mock_get, client):
        """Test dashboard add autocomplete functionality"""
        mock_get.return_value.json.return_value = {
            "datasets": [
                {
                    "name": "test-dataset",
                    "dataset": "test-id",
                    "collection": "test-collection",
                }
            ]
        }

        response = client.get("/datamanager/dashboard/add?autocomplete=test")
        assert response.status_code == 200

    @patch("requests.get")
    def test_dashboard_add_get_orgs_for(self, mock_get, client):
        """Test dashboard add get organizations functionality"""
        mock_get.side_effect = [
            MagicMock(
                json=lambda: {
                    "datasets": [
                        {
                            "name": "test-dataset",
                            "dataset": "test-id",
                            "collection": "test-collection",
                        }
                    ]
                }
            ),
            MagicMock(
                json=lambda: {
                    "rows": [
                        {
                            "organisation": {
                                "label": "Test Org",
                                "value": "local-authority:TEST",
                            }
                        }
                    ]
                }
            ),
        ]

        response = client.get("/datamanager/dashboard/add?get_orgs_for=test-dataset")
        assert response.status_code == 200

    @patch("requests.post")
    @patch("requests.get")
    def test_configure_post(self, mock_get, mock_post, client):
        """Test configure POST method"""
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {
            "params": {"dataset": "test", "url": "https://test.com"},
            "response": {"data": {}},
        }
        mock_post.return_value.status_code = 202
        mock_post.return_value.json.return_value = {"id": "new-request-id"}

        response = client.post(
            "/datamanager/configure/test-id",
            data={"map_raw[col1]": "reference", "geom_type": "point"},
        )
        assert response.status_code == 302


class TestUtilityFunctions:
    @patch("requests.get")
    def test_get_spec_fields_union(self, mock_get):
        """Test get_spec_fields_union function"""
        mock_get.return_value.json.return_value = [
            {"field": "reference"},
            {"field": "name"},
        ]
        mock_get.return_value.raise_for_status.return_value = None

        result = get_spec_fields_union("test-dataset")
        assert isinstance(result, list)
        assert "reference" in result

    @patch("requests.get")
    def test_read_raw_csv_preview(self, mock_get):
        """Test read_raw_csv_preview function"""
        mock_get.return_value.content = b"col1,col2\nval1,val2\n"

        headers, rows = read_raw_csv_preview("https://test.com/data.csv")
        assert headers == ["col1", "col2"]
        assert len(rows) == 1
        assert rows[0] == ["val1", "val2"]
