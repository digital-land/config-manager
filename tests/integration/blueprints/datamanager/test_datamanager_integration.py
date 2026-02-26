from unittest.mock import patch

import responses as rsps

ASYNC_BASE = "http://localhost:8000/requests"

PENDING_CHECK_RESULT = {
    "id": "test-id",
    "status": "PENDING",
    "response": None,
    "params": {
        "organisationName": "local-authority-eng:ABC",
        "dataset": "brownfield-land",
    },
}

PENDING_ADD_DATA_RESULT = {
    "id": "test-id",
    "status": "PENDING",
    "response": None,
    "params": {"dataset": "brownfield-land"},
}


class TestDashboardGet:
    def test_returns_200(self, client):
        response = client.get("/datamanager/")
        assert response.status_code == 200

    def test_contains_form(self, client):
        response = client.get("/datamanager/")
        assert b"<form" in response.data

    def test_autocomplete_returns_json(self, client):
        with patch(
            "application.blueprints.datamanager.controllers.form.search_datasets",
            return_value=["brownfield-land"],
        ):
            response = client.get("/datamanager/?autocomplete=brown")
        assert response.status_code == 200
        assert b"brownfield-land" in response.data


class TestImportRoute:
    def test_get_returns_200(self, client):
        response = client.get("/datamanager/import")
        assert response.status_code == 200

    def test_post_with_valid_csv_redirects(self, client):
        csv_data = (
            "organisation,pipelines,endpoint-url\n"
            "local-authority-eng:ABC,brownfield-land,https://example.com/data.csv"
        )
        response = client.post(
            "/datamanager/import", data={"mode": "parse", "csv_data": csv_data}
        )
        assert response.status_code == 302
        assert "import_data=true" in response.headers["Location"]

    def test_post_with_invalid_csv_shows_error(self, client):
        response = client.post(
            "/datamanager/import",
            data={
                "mode": "parse",
                "csv_data": "not,valid,csv\nmissing,required,fields",
            },
        )
        assert response.status_code == 200
        assert b"error" in response.data.lower()


class TestCheckResultsRoute:
    @rsps.activate
    def test_pending_renders_loading(self, client):
        rsps.add(
            rsps.GET, f"{ASYNC_BASE}/test-id", json=PENDING_CHECK_RESULT, status=200
        )
        with patch(
            "application.blueprints.datamanager.controllers.check.get_organisation_name",
            return_value="Test Org",
        ):
            with patch(
                "application.blueprints.datamanager.controllers.check.get_dataset_name",
                return_value="Brownfield Land",
            ):
                response = client.get("/datamanager/check-results/test-id")
        assert response.status_code == 200

    @rsps.activate
    def test_not_found_returns_404(self, client):
        rsps.add(
            rsps.GET,
            f"{ASYNC_BASE}/bad-id",
            json={"detail": {"errMsg": "not found"}},
            status=400,
        )
        response = client.get("/datamanager/check-results/bad-id")
        assert response.status_code == 404

    @rsps.activate
    def test_failed_status_returns_404(self, client):
        rsps.add(
            rsps.GET,
            f"{ASYNC_BASE}/test-id",
            json={**PENDING_CHECK_RESULT, "status": "FAILED"},
            status=200,
        )
        response = client.get("/datamanager/check-results/test-id")
        assert response.status_code == 404


class TestEntitiesPreviewRoute:
    @rsps.activate
    def test_pending_renders_loading(self, client):
        rsps.add(
            rsps.GET, f"{ASYNC_BASE}/test-id", json=PENDING_ADD_DATA_RESULT, status=200
        )
        response = client.get("/datamanager/add-data/test-id/entities")
        assert response.status_code == 200
        assert b"Preparing entities preview" in response.data

    @rsps.activate
    def test_not_found_returns_404(self, client):
        rsps.add(
            rsps.GET,
            f"{ASYNC_BASE}/bad-id",
            json={"detail": {"errMsg": "not found"}},
            status=400,
        )
        response = client.get("/datamanager/add-data/bad-id/entities")
        assert response.status_code == 404

    @rsps.activate
    def test_queued_also_renders_loading(self, client):
        queued = {**PENDING_ADD_DATA_RESULT, "status": "QUEUED"}
        rsps.add(rsps.GET, f"{ASYNC_BASE}/test-id", json=queued, status=200)
        response = client.get("/datamanager/add-data/test-id/entities")
        assert response.status_code == 200
        assert b"Preparing entities preview" in response.data


class TestAddDataConfirmRoute:
    def test_success_renders_success_page(self, client):
        with client.session_transaction() as sess:
            sess["user"] = {"login": "test-user"}
        with patch(
            "application.blueprints.datamanager.controllers.add.trigger_add_data_async_workflow",
            return_value={"success": True, "message": "Workflow triggered"},
        ):
            response = client.post("/datamanager/add-data/test-id/confirm-async")
        assert response.status_code == 200
        assert b"triggered" in response.data.lower()

    def test_workflow_failure_renders_error(self, client):
        with client.session_transaction() as sess:
            sess["user"] = {"login": "test-user"}
        with patch(
            "application.blueprints.datamanager.controllers.add.trigger_add_data_async_workflow",
            return_value={"success": False, "message": "Dispatch rejected"},
        ):
            response = client.post("/datamanager/add-data/test-id/confirm-async")
        assert response.status_code == 200
        assert b"govuk-error-summary" in response.data

    def test_github_error_renders_error(self, client):
        from application.blueprints.datamanager.services.github import (
            GitHubWorkflowError,
        )

        with client.session_transaction() as sess:
            sess["user"] = {"login": "test-user"}
        with patch(
            "application.blueprints.datamanager.controllers.add.trigger_add_data_async_workflow",
            side_effect=GitHubWorkflowError("credentials not configured"),
        ):
            response = client.post("/datamanager/add-data/test-id/confirm-async")
        assert response.status_code == 200
        assert b"govuk-error-summary" in response.data
