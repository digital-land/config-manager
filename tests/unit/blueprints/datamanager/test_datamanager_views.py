import pytest
import json
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


class TestSpecificLines:
    """Tests for specific line coverage"""

    @patch("application.blueprints.datamanager.views.requests.get")
    def test_read_raw_csv_preview_out_sort(self, mock_get):
        """Test line 97: out.sort(key=lambda x: x.lower())"""
        mock_response = Mock()
        mock_response.json.return_value = [
            {"field": "ZField"},
            {"field": "AField"},
            {"field": "MField"},
        ]
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        result = get_spec_fields_union("test-dataset")
        assert result == ["AField", "MField", "ZField"]  # Should be sorted

    @patch("application.blueprints.datamanager.views.requests.get")
    def test_dashboard_add_provision_exception(self, mock_get, client):
        """Test lines 139-141: provision_rows exception handling"""
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

        def side_effect_func(url, *args, **kwargs):
            if "provision.json" in url:
                raise Exception("Network error")
            return dataset_response

        mock_get.side_effect = side_effect_func

        response = client.get("/datamanager/dashboard/add?get_orgs_for=test-dataset")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data == []

    @patch("application.blueprints.datamanager.views.requests.get")
    def test_dashboard_add_selected_orgs_list_comprehension(self, mock_get, client):
        """Test line 166: selected_orgs list comprehension"""
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

        provision_response = Mock()
        provision_response.json.return_value = {
            "rows": [
                {"organisation": {"label": "Test Org 1", "value": "prefix:TEST1"}},
                {"organisation": {"label": "Test Org 2", "value": "prefix:TEST2"}},
            ]
        }

        mock_get.side_effect = [dataset_response, provision_response]

        response = client.get("/datamanager/dashboard/add?get_orgs_for=test-dataset")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert "Test Org 1 (TEST1)" in data
        assert "Test Org 2 (TEST2)" in data

    @patch("application.blueprints.datamanager.views.requests.get")
    def test_ldashboard_add_provision_exception_in_post(self, mock_get, client):
        """Test lines 181-182: provision exception in POST"""
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

        def side_effect_func(url, *args, **kwargs):
            if "provision.json" in url:
                raise Exception("Network error")
            return dataset_response

        mock_get.side_effect = side_effect_func

        form_data = {
            "mode": "final",
            "dataset": "test-dataset",
            "organisation": "Test Org",
            "endpoint_url": "https://example.com/data.csv",
        }

        response = client.post("/datamanager/dashboard/add", data=form_data)
        assert response.status_code == 200

    @patch("application.blueprints.datamanager.views.requests.get")
    def test_dashboard_add_dashboard_add_post_complex(self, mock_get, client):
        """Test lines 197-383: Complex POST logic"""
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

        # Test with org_warning
        form_data = {
            "mode": "final",
            "dataset": "test-dataset",
            "organisation": "Unknown Org",
            "endpoint_url": "https://example.com/data.csv",
            "org_warning": "true",
        }

        response = client.post("/datamanager/dashboard/add", data=form_data)
        assert response.status_code == 200

    @patch("application.blueprints.datamanager.views.requests.get")
    @patch("application.blueprints.datamanager.views.get_request_api_endpoint")
    def test_check_results_check_results_complex(self, mock_endpoint, mock_get, client):
        """Test lines 397-583: Complex check results logic"""
        mock_endpoint.return_value = "http://test-api"

        main_response = Mock()
        main_response.status_code = 200
        main_response.json.return_value = {
            "status": "COMPLETED",
            "response": {
                "data": {
                    "entity-summary": {"existing-in-resource": 5, "new-in-resource": 3},
                    "new-entities": [{"reference": "ref1", "entity": "ent1"}],
                    "existing-entities": [{"reference": "ref2", "entity": "ent2"}],
                    "error-summary": [],
                    "column-field-log": [
                        {"field": "required_field", "missing": False},
                        {"field": "missing_field", "missing": True},
                    ],
                    "row-count": 10,
                }
            },
            "params": {
                "organisation": "local-authority-eng:ABC123",
                "documentation_url": None,
                "licence": None,
                "start_date": None,
                "url": "https://example.com/data.csv",
            },
        }
        main_response.raise_for_status.return_value = None

        entity_response = Mock()
        entity_response.json.return_value = {
            "entities": [
                {"reference": "E12345678", "local-planning-authority": "ABC123"}
            ]
        }
        entity_response.raise_for_status.return_value = None

        boundary_response = Mock()
        boundary_response.json.return_value = {
            "type": "FeatureCollection",
            "features": [],
        }

        def mock_get_side_effect(url, *args, **kwargs):
            if "response-details" in url:
                details_response = Mock()
                details_response.status_code = 200
                details_response.json.return_value = [
                    {
                        "converted_row": {"Reference": "REF001", "Point": "POINT(1 2)"},
                        "geometry": None,
                        "entry_number": 1,
                    }
                ]
                details_response.raise_for_status.return_value = None
                return details_response
            elif "entity.json" in url:
                return entity_response
            elif "entity.geojson" in url:
                return boundary_response
            return main_response

        mock_get.side_effect = mock_get_side_effect

        response = client.get("/datamanager/check-results/test-id")
        assert response.status_code == 200

    @patch("application.blueprints.datamanager.views.requests.patch")
    @patch("application.blueprints.datamanager.views.get_request_api_endpoint")
    def test_optional_fields_submit_optional_fields_submit_complex(
        self, mock_endpoint, mock_patch, client
    ):
        """Test lines 588-620: Optional fields submit logic"""
        mock_endpoint.return_value = "http://test-api"
        mock_patch.return_value = Mock()

        form_data = {
            "request_id": "test-id",
            "documentation_url": "https://example.gov.uk",
            "licence": "ogl",
            "start_day": "15",
            "start_month": "6",
            "start_year": "2024",
        }

        response = client.post(
            "/datamanager/check-results/optional-submit", data=form_data
        )
        assert response.status_code == 302

    @patch("application.blueprints.datamanager.views.requests.post")
    @patch("application.blueprints.datamanager.views.get_request_api_endpoint")
    def test_lines_add_data_add_data_complex(self, mock_endpoint, mock_post, client):
        """Test lines 625-709: Add data complex logic"""
        mock_endpoint.return_value = "http://test-api"

        # Test _submit_preview function
        mock_response = Mock()
        mock_response.status_code = 202
        mock_response.json.return_value = {"id": "preview-id"}
        mock_post.return_value = mock_response

        with client.session_transaction() as sess:
            sess["required_fields"] = {"collection": "test", "dataset": "test"}

        form_data = {
            "documentation_url": "https://example.gov.uk",
            "licence": "ogl",
            "start_day": "1",
            "start_month": "1",
            "start_year": "2024",
        }

        response = client.post("/datamanager/check-results/add-data", data=form_data)
        assert response.status_code == 302

    @patch("application.blueprints.datamanager.views.requests.get")
    @patch("application.blueprints.datamanager.views.requests.post")
    @patch("application.blueprints.datamanager.views.get_request_api_endpoint")
    def test_add_data_confirm_add_data_confirm_complex(
        self, mock_endpoint, mock_post, mock_get, client
    ):
        """Test lines 714-742: Add data confirm logic"""
        mock_endpoint.return_value = "http://test-api"

        get_response = Mock()
        get_response.status_code = 200
        get_response.json.return_value = {
            "params": {"type": "add_data", "preview": True}
        }
        mock_get.return_value = get_response

        post_response = Mock()
        post_response.status_code = 202
        post_response.json.return_value = {"id": "new-id", "message": "Success"}
        mock_post.return_value = post_response

        with client.session_transaction() as sess:
            sess["optional_fields"] = {"test": "data"}

        response = client.post("/datamanager/check-results/test-id/add-data/confirm")
        assert response.status_code == 302

    @patch("application.blueprints.datamanager.views.render_template")
    @patch("application.blueprints.datamanager.views.requests.get")
    @patch("application.blueprints.datamanager.views.get_request_api_endpoint")
    def test_configure_configure_complex(
        self, mock_endpoint, mock_get, mock_render, client
    ):
        """Test lines : Configure complex logic"""
        mock_endpoint.return_value = "http://test-api"
        mock_render.return_value = "<html>Configure page</html>"

        main_response = Mock()
        main_response.status_code = 200
        main_response.json.return_value = {
            "params": {
                "dataset": "test-dataset",
                "url": "https://example.com/data.csv",
                "column_mapping": {"raw_field": "spec_field"},
            },
            "response": {
                "data": {
                    "column-field-log": [{"field": "required_field", "missing": True}],
                    "column-mapping": {"existing_raw": "existing_spec"},
                }
            },
            "status": "COMPLETED",
        }

        csv_response = Mock()
        csv_response.content = b"raw_field,another_field\nvalue1,value2"

        details_response = Mock()
        details_response.status_code = 200
        details_response.json.return_value = [
            {"converted_row": {"spec_field": "converted_value"}}
        ]

        mock_get.side_effect = [main_response, csv_response, details_response]

        response = client.get("/datamanager/configure/test-id")
        assert response.status_code == 200

    @patch("application.blueprints.datamanager.views.render_template")
    def test_add_data_result_add_data_progress_with_message(self, mock_render, client):
        """Test lines 987-988: Add data progress with custom message"""
        mock_render.return_value = "<html>Progress page</html>"
        response = client.get(
            "/datamanager/add-data/progress/test-id?msg=Custom%20message"
        )
        assert response.status_code == 200

    @patch("application.blueprints.datamanager.views.render_template")
    @patch("application.blueprints.datamanager.views.requests.get")
    @patch("application.blueprints.datamanager.views.get_request_api_endpoint")
    def test_add_data_result_add_data_result_complex(
        self, mock_endpoint, mock_get, mock_render, client
    ):
        """Test lines 995-1055: Add data result complex logic"""
        mock_endpoint.return_value = "http://test-api"
        mock_render.return_value = "<html>Result page</html>"

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "status": "COMPLETED",
            "response": {
                "data": {
                    "new-entity-count": 5,
                    "min-entity-after": "100",
                    "max-entity-after": "200",
                    "lookup-path": "/path/to/lookup",
                    "new-entities": [
                        {
                            "reference": "ref1",
                            "prefix": "pre1",
                            "organisation": "org1",
                            "entity": "ent1",
                        },
                        {
                            "reference": "ref2",
                            "prefix": "pre2",
                            "organisation": "org2",
                            "entity": "ent2",
                        },
                    ],
                }
            },
        }
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        response = client.get("/datamanager/add-data/result/test-id")
        assert response.status_code == 200

    @patch("application.blueprints.datamanager.views.requests.get")
    @patch("application.blueprints.datamanager.views.get_request_api_endpoint")
    def test_entities_preview_entities_preview_complex(
        self, mock_endpoint, mock_get, client
    ):
        """Test lines 1065-1202: Entities preview complex logic"""
        mock_endpoint.return_value = "http://test-api"

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "status": "COMPLETED",
            "response": {
                "data": {
                    "entity-summary": {
                        "existing-in-resource": 5,
                        "new-in-resource": 3,
                        "existing-entity-breakdown": [
                            {"reference": "existing1", "entity": "ent1"}
                        ],
                    },
                    "new-entities": [
                        {
                            "reference": "new1",
                            "prefix": "pre1",
                            "organisation": "org1",
                            "entity": "ent1",
                        }
                    ],
                    "endpoint_url_validation": {
                        "found_in_endpoint_csv": True,
                        "existing_row": {"endpoint": "ep1", "endpoint-url": "url1"},
                        "new_endpoint_entry": {
                            "endpoint": "ep2",
                            "endpoint-url": "url2",
                        },
                        "new_source_entry": {
                            "source": "src1",
                            "attribution": "attr1",
                            "collection": "col1",
                            "documentation-url": "doc1",
                            "endpoint": "ep1",
                            "licence": "lic1",
                            "organisation": "org1",
                            "pipelines": "pipe1",
                            "entry-date": "2024-01-01",
                            "start-date": "2024-01-01",
                            "end-date": "2024-12-31",
                        },
                        "columns": ["custom-endpoint", "custom-url"],
                    },
                    "new-entity-breakdown": [{"type": "new", "count": 3}],
                }
            },
            "params": {"source_request_id": "source-123"},
        }
        mock_get.return_value = mock_response

        response = client.get("/datamanager/check-results/test-id/entities")
        assert response.status_code == 200

    @pytest.mark.skip("Skipping as requested")
    @patch("application.blueprints.datamanager.views.render_template")
    @patch("application.blueprints.datamanager.views.requests.get")
    @patch("application.blueprints.datamanager.views.get_request_api_endpoint")
    def test_configure_table_from_csv_empty(
        self, mock_endpoint, mock_get, mock_render, client
    ):
        """Test table_from_csv with empty data"""
        mock_endpoint.return_value = "http://test-api"
        mock_render.return_value = "<html>Configure page</html>"

        main_response = Mock()
        main_response.status_code = 200
        main_response.json.return_value = {
            "params": {
                "dataset": "test-dataset",
                "url": "https://example.com/data.csv",
            },
            "response": {"data": {}},
            "status": "COMPLETED",
        }

        csv_response = Mock()
        csv_response.content = b""  # Empty CSV

        mock_get.side_effect = [main_response, csv_response]

        response = client.get("/datamanager/configure/test-id")
        assert response.status_code == 200

    @patch("application.blueprints.datamanager.views.requests.get")
    @patch("application.blueprints.datamanager.views.get_request_api_endpoint")
    def test_check_results_wkt_geometry_conversion(
        self, mock_endpoint, mock_get, client
    ):
        """Test WKT geometry conversion in check results"""
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
            "params": {
                "organisation": "local-authority-eng:ABC123",
                "url": "https://example.com/data.csv",
            },
        }
        main_response.raise_for_status.return_value = None

        entity_response = Mock()
        entity_response.json.return_value = {
            "entities": [
                {"reference": "E12345678", "local-planning-authority": "ABC123"}
            ]
        }
        entity_response.raise_for_status.return_value = None

        boundary_response = Mock()
        boundary_response.json.return_value = {
            "type": "FeatureCollection",
            "features": [],
        }

        def mock_get_side_effect(url, *args, **kwargs):
            if "response-details" in url:
                details_response = Mock()
                details_response.status_code = 200
                details_response.json.return_value = [
                    {
                        "converted_row": {
                            "Reference": "REF001",
                            "Point": "POINT(1.0 2.0)",
                        },
                        "geometry": None,
                        "entry_number": 1,
                    }
                ]
                details_response.raise_for_status.return_value = None
                return details_response
            elif "entity.json" in url:
                return entity_response
            elif "entity.geojson" in url:
                return boundary_response
            return main_response

        mock_get.side_effect = mock_get_side_effect

        response = client.get("/datamanager/check-results/test-id")
        assert response.status_code == 200

    @patch("application.blueprints.datamanager.views.requests.get")
    @patch("application.blueprints.datamanager.views.get_request_api_endpoint")
    def test_check_results_wkt_geometry_exception(
        self, mock_endpoint, mock_get, client
    ):
        """Test WKT geometry conversion exception handling"""
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
            "params": {
                "organisation": "local-authority-eng:ABC123",
                "url": "https://example.com/data.csv",
            },
        }
        main_response.raise_for_status.return_value = None

        entity_response = Mock()
        entity_response.json.return_value = {
            "entities": [
                {"reference": "E12345678", "local-planning-authority": "ABC123"}
            ]
        }
        entity_response.raise_for_status.return_value = None

        boundary_response = Mock()
        boundary_response.json.return_value = {
            "type": "FeatureCollection",
            "features": [],
        }

        def mock_get_side_effect(url, *args, **kwargs):
            if "response-details" in url:
                details_response = Mock()
                details_response.status_code = 200
                details_response.json.return_value = [
                    {
                        "converted_row": {
                            "Reference": "REF001",
                            "Point": "INVALID_WKT",
                        },
                        "geometry": None,
                        "entry_number": 1,
                    }
                ]
                details_response.raise_for_status.return_value = None
                return details_response
            elif "entity.json" in url:
                return entity_response
            elif "entity.geojson" in url:
                return boundary_response
            return main_response

        mock_get.side_effect = mock_get_side_effect

        response = client.get("/datamanager/check-results/test-id")
        assert response.status_code == 200

    @patch("application.blueprints.datamanager.views.requests.post")
    @patch("application.blueprints.datamanager.views.requests.get")
    @patch("application.blueprints.datamanager.views.get_request_api_endpoint")
    def test_dashboard_add_payload_creation_and_api_submission(
        self, mock_endpoint, mock_get, mock_post, client
    ):
        """Test lines 316-383: Payload creation, session management, and API submission"""
        mock_endpoint.return_value = "http://test-api"

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
        # Mock provision response for organization mapping
        provision_response = Mock()
        provision_response.json.return_value = {
            "rows": [
                {
                    "organisation": {
                        "label": "Test Org",
                        "value": "local-authority-eng:ABC123",
                    }
                }
            ]
        }
        mock_get.side_effect = [dataset_response, provision_response]

        # Mock successful API response
        api_response = Mock()
        api_response.status_code = 202
        api_response.json.return_value = {"id": "test-request-id"}
        mock_post.return_value = api_response

        form_data = {
            "mode": "final",
            "dataset": "test-dataset",
            "organisation": "Test Org (ABC123)",
            "endpoint_url": "https://example.com/data.csv",
            "documentation_url": "https://example.gov.uk/docs",
            "licence": "ogl",
            "start_day": "1",
            "start_month": "1",
            "start_year": "2024",
            "column_mapping": '{"raw_field": "spec_field"}',
            "geom_type": "point",
        }

        with client.session_transaction() as sess:
            # Clear any existing session data
            sess.clear()

        response = client.post("/datamanager/dashboard/add", data=form_data)

        # Verify redirect response
        assert response.status_code == 302

        # Verify API was called with correct payload
        mock_post.assert_called_once()
        call_args = mock_post.call_args
        payload = call_args[1]["json"]

        # Verify payload structure
        assert payload["params"]["type"] == "check_url"
        assert payload["params"]["collection"] == "test-collection"
        assert payload["params"]["dataset"] == "test-id"
        assert payload["params"]["url"] == "https://example.com/data.csv"
        assert payload["params"]["documentation_url"] == "https://example.gov.uk/docs"
        assert payload["params"]["licence"] == "ogl"
        assert payload["params"]["start_date"] == "2024-01-01"
        assert payload["params"]["organisation"] == "local-authority-eng:ABC123"
        assert payload["params"]["column_mapping"] == {"raw_field": "spec_field"}
        assert payload["params"]["geom_type"] == "point"

        # Verify session data was set
        with client.session_transaction() as sess:
            assert "required_fields" in sess
            assert "optional_fields" in sess
            assert sess["required_fields"]["collection"] == "test-collection"
            assert sess["required_fields"]["dataset"] == "test-id"
            assert sess["required_fields"]["url"] == "https://example.com/data.csv"
            assert (
                sess["required_fields"]["organisation"] == "local-authority-eng:ABC123"
            )
            assert (
                sess["optional_fields"]["documentation_url"]
                == "https://example.gov.uk/docs"
            )
            assert sess["optional_fields"]["licence"] == "ogl"
            assert sess["optional_fields"]["start_date"] == "2024-01-01"

    @patch("application.blueprints.datamanager.views.requests.post")
    @patch("application.blueprints.datamanager.views.requests.get")
    @patch("application.blueprints.datamanager.views.get_request_api_endpoint")
    def test_dashboard_add_api_error_handling(
        self, mock_endpoint, mock_get, mock_post, client
    ):
        """Test lines 373-383: API error handling for non-202 responses and exceptions"""
        mock_endpoint.return_value = "http://test-api"

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
            "rows": [
                {
                    "organisation": {
                        "label": "Test Org",
                        "value": "local-authority-eng:ABC123",
                    }
                }
            ]
        }

        mock_get.side_effect = [dataset_response, provision_response]

        # Test case 1: API returns 500 error with JSON response
        api_response = Mock()
        api_response.status_code = 500
        api_response.json.return_value = {
            "error": "Internal server error",
            "details": "Database connection failed",
        }
        mock_post.return_value = api_response

        form_data = {
            "mode": "final",
            "dataset": "test-dataset",
            "organisation": "Test Org (ABC123)",
            "endpoint_url": "https://example.com/data.csv",
        }

        response = client.post("/datamanager/dashboard/add", data=form_data)
        assert response.status_code == 500

    @patch("application.blueprints.datamanager.views.requests.post")
    @patch("application.blueprints.datamanager.views.requests.get")
    @patch("application.blueprints.datamanager.views.get_request_api_endpoint")
    def test_dashboard_add_api_json_decode_error(
        self, mock_endpoint, mock_get, mock_post, client
    ):
        """Test lines 373-383: API error handling when JSON decode fails"""
        mock_endpoint.return_value = "http://test-api"

        # Mock dataset and provision responses
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

        provision_response = Mock()
        provision_response.json.return_value = {
            "rows": [
                {
                    "organisation": {
                        "label": "Test Org",
                        "value": "local-authority-eng:ABC123",
                    }
                }
            ]
        }

        mock_get.side_effect = [dataset_response, provision_response]

        # Test case 2: API returns 400 error with invalid JSON (falls back to text)
        api_response = Mock()
        api_response.status_code = 400
        api_response.json.side_effect = Exception("Invalid JSON")
        api_response.text = "Bad Request: Invalid parameters"
        mock_post.return_value = api_response

        form_data = {
            "mode": "final",
            "dataset": "test-dataset",
            "organisation": "Test Org (ABC123)",
            "endpoint_url": "https://example.com/data.csv",
        }

        response = client.post("/datamanager/dashboard/add", data=form_data)
        assert response.status_code == 500

    @patch("application.blueprints.datamanager.views.requests.post")
    @patch("application.blueprints.datamanager.views.requests.get")
    @patch("application.blueprints.datamanager.views.get_request_api_endpoint")
    def test_dashboard_add_network_exception(
        self, mock_endpoint, mock_get, mock_post, client
    ):
        """Test lines 373-383: Exception handling during API call"""
        mock_endpoint.return_value = "http://test-api"

        # Mock dataset and provision responses
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

        provision_response = Mock()
        provision_response.json.return_value = {
            "rows": [
                {
                    "organisation": {
                        "label": "Test Org",
                        "value": "local-authority-eng:ABC123",
                    }
                }
            ]
        }

        mock_get.side_effect = [dataset_response, provision_response]

        # Test case 3: Network exception during API call
        mock_post.side_effect = Exception("Connection timeout")

        form_data = {
            "mode": "final",
            "dataset": "test-dataset",
            "organisation": "Test Org (ABC123)",
            "endpoint_url": "https://example.com/data.csv",
        }

        response = client.post("/datamanager/dashboard/add", data=form_data)
        assert response.status_code == 500

        """Test get_statistical_geography function with exception """
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
                "params": {"organisation": "test-org"},
            }

            details_response = Mock()
            details_response.status_code = 200
            details_response.json.return_value = []
            details_response.raise_for_status.return_value = None

            # Third call (organisation) raises exception
            mock_get.side_effect = [
                main_response,
                details_response,
                Exception("Network error"),
            ]

            with patch(
                "application.blueprints.datamanager.views.render_template"
            ) as mock_render:
                mock_render.return_value = "rendered_template"
                from application.blueprints.datamanager.views import check_results

                result = check_results("test-id")
                assert result is not None

    @pytest.mark.skip("Skipping as requested")
    @patch("application.blueprints.datamanager.views.requests.get")
    def test_boundary_url_generation(self, mock_get, client):
        """Test boundary URL generation logic"""
        with patch(
            "application.blueprints.datamanager.views.get_request_api_endpoint"
        ) as mock_endpoint:
            mock_endpoint.return_value = "http://test-api"

            # Mock main response
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

            # Mock details response
            details_response = Mock()
            details_response.status_code = 200
            details_response.json.return_value = []
            details_response.raise_for_status.return_value = None

            # Mock entity response
            entity_response = Mock()
            entity_response.json.return_value = {
                "entities": [
                    {"reference": "E12345678", "local-planning-authority": "ABC123"}
                ]
            }
            entity_response.raise_for_status.return_value = None

            # Mock boundary response
            boundary_response = Mock()
            boundary_response.json.return_value = {
                "type": "FeatureCollection",
                "features": [],
            }

            mock_get.side_effect = [
                main_response,
                details_response,
                entity_response,
                boundary_response,
            ]

            response = client.get("/datamanager/check-results/test-id")
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
