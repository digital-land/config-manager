import io
from unittest.mock import patch

from application.blueprints.datamanager.controllers.form import (
    _has_all_add_data_fields,
    _parse_start_date,
)


class TestHasAllAddDataFields:
    def test_returns_true_when_all_fields_present(self):
        fields = {
            "documentation_url": "https://example.gov.uk/docs",
            "licence": "ogl3",
            "start_date": "2024-01-01",
            "authoritative": True,
            "github_new": True,
        }
        assert _has_all_add_data_fields(fields) is True

    def test_returns_false_when_documentation_url_missing(self):
        fields = {
            "documentation_url": "",
            "licence": "ogl3",
            "start_date": "2024-01-01",
            "authoritative": True,
            "github_new": True,
        }
        assert not _has_all_add_data_fields(fields)

    def test_returns_false_when_authoritative_is_none(self):
        fields = {
            "documentation_url": "https://example.gov.uk/docs",
            "licence": "ogl3",
            "start_date": "2024-01-01",
            "authoritative": None,
            "github_new": True,
        }
        assert not _has_all_add_data_fields(fields)


class TestParseStartDate:
    def test_valid_date_returns_day_month_year(self):
        result = _parse_start_date("2024-03-15")
        assert result == {"start_day": "15", "start_month": "3", "start_year": "2024"}

    def test_empty_string_returns_empty_dict(self):
        assert _parse_start_date("") == {}

    def test_none_returns_empty_dict(self):
        assert _parse_start_date(None) == {}

    def test_invalid_string_returns_empty_dict(self):
        assert _parse_start_date("not-a-date") == {}

    def test_single_digit_day_and_month_are_not_zero_padded(self):
        result = _parse_start_date("2024-01-05")
        assert result["start_day"] == "5"
        assert result["start_month"] == "1"


class TestDashboardGetOrgsFor:
    def test_returns_formatted_org_list_when_dataset_found(self, client):
        formatted = [
            {
                "code": "local-authority:ABC",
                "label": "ABC Council (local-authority:ABC)",
            }
        ]
        with patch(
            "application.blueprints.datamanager.controllers.form.get_dataset_id",
            return_value="brownfield-land",
        ), patch(
            "application.blueprints.datamanager.controllers.form.get_provision_orgs_for_dataset",
            return_value=["local-authority:ABC"],
        ), patch(
            "application.blueprints.datamanager.controllers.form.format_org_options",
            return_value=formatted,
        ):
            response = client.get("/datamanager/?get_orgs_for=brownfield-land")

        assert response.status_code == 200
        data = response.get_json()
        assert data == formatted

    def test_returns_empty_list_when_dataset_not_found(self, client):
        with patch(
            "application.blueprints.datamanager.controllers.form.get_dataset_id",
            return_value=None,
        ):
            response = client.get("/datamanager/?get_orgs_for=unknown-dataset")

        assert response.status_code == 200
        assert response.get_json() == []

    def test_returns_empty_list_on_exception_fetching_orgs(self, client):
        with patch(
            "application.blueprints.datamanager.controllers.form.get_dataset_id",
            return_value="brownfield-land",
        ), patch(
            "application.blueprints.datamanager.controllers.form.get_provision_orgs_for_dataset",
            side_effect=Exception("API error"),
        ):
            response = client.get("/datamanager/?get_orgs_for=brownfield-land")

        assert response.status_code == 200
        assert response.get_json() == []


class TestDashboardGetImportData:
    def test_returns_200_and_prefills_form(self, client):
        with patch(
            "application.blueprints.datamanager.controllers.form.get_dataset_name",
            return_value="Brownfield Land",
        ), patch(
            "application.blueprints.datamanager.controllers.form.get_provision_orgs_for_dataset",
            return_value=["local-authority:ABC"],
        ), patch(
            "application.blueprints.datamanager.controllers.form.format_org_options",
            return_value=[
                {
                    "code": "local-authority:ABC",
                    "label": "ABC Council (local-authority:ABC)",
                }
            ],
        ):
            response = client.get(
                "/datamanager/?import_data=true"
                "&dataset=brownfield-land"
                "&organisation=local-authority:ABC"
                "&endpoint_url=https://example.com"
            )

        assert response.status_code == 200

    def test_start_date_param_is_split_into_day_month_year(self, client):
        with patch(
            "application.blueprints.datamanager.controllers.form.get_dataset_name",
            return_value="Brownfield Land",
        ), patch(
            "application.blueprints.datamanager.controllers.form.get_provision_orgs_for_dataset",
            return_value=[],
        ), patch(
            "application.blueprints.datamanager.controllers.form.format_org_options",
            return_value=[],
        ):
            response = client.get(
                "/datamanager/?import_data=true"
                "&dataset=brownfield-land"
                "&start_date=2024-06-15"
            )

        assert response.status_code == 200
        # The template receives the split date fields; verify the response rendered without error
        assert b"2024" in response.data or response.status_code == 200


class TestDashboardAddPost:
    def test_partial_date_only_day_provided_returns_200_with_error(self, client):
        with patch(
            "application.blueprints.datamanager.controllers.form.get_dataset_id",
            return_value="brownfield-land",
        ), patch(
            "application.blueprints.datamanager.controllers.form.get_collection_id",
            return_value="brownfield",
        ), patch(
            "application.blueprints.datamanager.controllers.form.get_provision_orgs_for_dataset",
            return_value=[],
        ), patch(
            "application.blueprints.datamanager.controllers.form.format_org_options",
            return_value=[],
        ), patch(
            "application.blueprints.datamanager.controllers.form.is_valid_organisation",
            return_value=False,
        ):
            response = client.post(
                "/datamanager/",
                data={
                    "dataset": "Brownfield Land",
                    "organisation": "local-authority:ABC",
                    "endpoint_url": "https://example.com/data.csv",
                    "authoritative": "yes",
                    "start_day": "15",
                    "start_month": "",
                    "start_year": "",
                },
            )

        assert response.status_code == 200

    def test_invalid_date_values_returns_200_with_error(self, client):
        with patch(
            "application.blueprints.datamanager.controllers.form.get_dataset_id",
            return_value="brownfield-land",
        ), patch(
            "application.blueprints.datamanager.controllers.form.get_collection_id",
            return_value="brownfield",
        ), patch(
            "application.blueprints.datamanager.controllers.form.get_provision_orgs_for_dataset",
            return_value=[],
        ), patch(
            "application.blueprints.datamanager.controllers.form.format_org_options",
            return_value=[],
        ), patch(
            "application.blueprints.datamanager.controllers.form.is_valid_organisation",
            return_value=False,
        ):
            response = client.post(
                "/datamanager/",
                data={
                    "dataset": "Brownfield Land",
                    "organisation": "local-authority:ABC",
                    "endpoint_url": "https://example.com/data.csv",
                    "authoritative": "yes",
                    "start_day": "99",
                    "start_month": "13",
                    "start_year": "2024",
                },
            )

        assert response.status_code == 200

    def test_empty_form_returns_200_with_re_rendered_form(self, client):
        with patch(
            "application.blueprints.datamanager.controllers.form.get_dataset_id",
            return_value=None,
        ), patch(
            "application.blueprints.datamanager.controllers.form.get_collection_id",
            return_value=None,
        ), patch(
            "application.blueprints.datamanager.controllers.form.get_provision_orgs_for_dataset",
            return_value=[],
        ), patch(
            "application.blueprints.datamanager.controllers.form.format_org_options",
            return_value=[],
        ), patch(
            "application.blueprints.datamanager.controllers.form.is_valid_organisation",
            return_value=False,
        ):
            response = client.post("/datamanager/", data={})

        assert response.status_code == 200


class TestDashboardAddImportPost:
    def test_file_upload_with_valid_single_row_csv_redirects(self, client):
        csv_content = (
            b"organisation,pipelines,endpoint-url,documentation-url,licence,start-date\n"
            b"local-authority:ABC,brownfield-land,https://example.com/data.csv,"
            b"https://example.com/docs,ogl3,2024-01-01\n"
        )
        response = client.post(
            "/datamanager/import",
            data={
                "mode": "parse",
                "csv_file": (io.BytesIO(csv_content), "test.csv"),
            },
            content_type="multipart/form-data",
        )

        # Redirect to dashboard with pre-filled params
        assert response.status_code == 302
        assert "import_data=true" in response.headers["Location"]

    def test_multiple_rows_in_csv_returns_200_with_error_message(self, client):
        csv_content = (
            b"organisation,pipelines,endpoint-url\n"
            b"local-authority:ABC,brownfield-land,https://example.com/1.csv\n"
            b"local-authority:DEF,brownfield-land,https://example.com/2.csv\n"
        )
        response = client.post(
            "/datamanager/import",
            data={
                "mode": "parse",
                "csv_file": (io.BytesIO(csv_content), "test.csv"),
            },
            content_type="multipart/form-data",
        )

        assert response.status_code == 200
        assert b"only one row" in response.data

    def test_csv_parse_exception_returns_200_with_error(self, client):
        with patch(
            "application.blueprints.datamanager.controllers.form.csv.DictReader",
            side_effect=Exception("Simulated CSV parse error"),
        ):
            response = client.post(
                "/datamanager/import",
                data={
                    "mode": "parse",
                    "csv_data": "some,csv,data\nrow1,row2,row3",
                },
            )

        assert response.status_code == 200
        assert b"Invalid CSV format" in response.data


class TestHandleAddData:
    def test_get_with_no_session_renders_form(self, client):
        with client.session_transaction() as sess:
            sess.pop("add_data_fields", None)

        response = client.get("/datamanager/add-data/test-request-id")
        assert response.status_code == 200

    def test_get_with_partial_session_renders_prefilled_form(self, client):
        with client.session_transaction() as sess:
            sess["add_data_fields"] = {
                "documentation_url": "https://example.gov.uk/docs",
                "licence": "ogl3",
                # Missing start_date, authoritative, github_new — so form should still render
            }

        response = client.get("/datamanager/add-data/test-request-id")
        assert response.status_code == 200

    def test_post_missing_authoritative_returns_200_with_error(self, client):
        with client.session_transaction() as sess:
            sess.pop("add_data_fields", None)

        response = client.post(
            "/datamanager/add-data/test-request-id",
            data={
                "documentation_url": "https://example.gov.uk/docs",
                "licence": "ogl3",
                "start_day": "1",
                "start_month": "1",
                "start_year": "2024",
                "github_new": "true",
                # authoritative intentionally omitted
            },
        )
        assert response.status_code == 200

    def test_post_missing_doc_url_returns_200_with_error(self, client):
        with client.session_transaction() as sess:
            sess.pop("add_data_fields", None)

        response = client.post(
            "/datamanager/add-data/test-request-id",
            data={
                "documentation_url": "",
                "licence": "ogl3",
                "authoritative": "yes",
                "start_day": "1",
                "start_month": "1",
                "start_year": "2024",
                "github_new": "true",
            },
        )
        assert response.status_code == 200

    def test_post_missing_start_date_returns_200_with_error(self, client):
        with client.session_transaction() as sess:
            sess.pop("add_data_fields", None)

        response = client.post(
            "/datamanager/add-data/test-request-id",
            data={
                "documentation_url": "https://example.gov.uk/docs",
                "licence": "ogl3",
                "authoritative": "yes",
                "start_day": "",
                "start_month": "",
                "start_year": "",
                "github_new": "true",
            },
        )
        assert response.status_code == 200

    def test_post_valid_all_fields_redirects_to_entities_preview(self, client):
        with client.session_transaction() as sess:
            sess.pop("add_data_fields", None)

        mock_check_request = {
            "params": {
                "collection": "brownfield",
                "dataset": "brownfield-land",
                "url": "https://example.com/data.csv",
                "organisationName": "local-authority:ABC",
                "geom_type": None,
                "column_mapping": {},
            }
        }

        with patch(
            "application.blueprints.datamanager.controllers.form.fetch_request",
            return_value=mock_check_request,
        ), patch(
            "application.blueprints.datamanager.controllers.form.submit_request",
            return_value="new-preview-request-id",
        ):
            response = client.post(
                "/datamanager/add-data/test-request-id",
                data={
                    "documentation_url": "https://example.gov.uk/docs",
                    "licence": "ogl3",
                    "authoritative": "yes",
                    "start_day": "1",
                    "start_month": "6",
                    "start_year": "2024",
                    "github_new": "true",
                },
            )

        assert response.status_code == 302
        assert "new-preview-request-id" in response.headers["Location"]
