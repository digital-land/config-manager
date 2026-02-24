from unittest.mock import patch

from application.blueprints.datamanager.services.github import GitHubWorkflowError


PENDING_ADD_DATA_RESULT = {
    "status": "PENDING",
    "response": None,
    "params": {"dataset": "brownfield-land"},
}


class TestEntitiesPreviewRoute:
    def test_renders_loading_template_when_pending(self, client):
        with patch("application.blueprints.datamanager.router.fetch_request", return_value=PENDING_ADD_DATA_RESULT):
            response = client.get("/datamanager/add-data/test-id/entities")
        assert response.status_code == 200
        assert b"Preparing entities preview" in response.data


class TestAddDataConfirmRoute:
    def test_renders_success_when_workflow_triggered(self, client):
        with client.session_transaction() as sess:
            sess["user"] = {"login": "test-user"}
        with patch(
            "application.blueprints.datamanager.controllers.add.trigger_add_data_async_workflow",
            return_value={"success": True, "message": "Workflow triggered"},
        ):
            response = client.post("/datamanager/add-data/test-id/confirm-async")
        assert response.status_code == 200
        assert b"triggered" in response.data.lower() or b"success" in response.data.lower()

    def test_returns_error_when_workflow_raises(self, client):
        with client.session_transaction() as sess:
            sess["user"] = {"login": "test-user"}
        with patch(
            "application.blueprints.datamanager.controllers.add.trigger_add_data_async_workflow",
            side_effect=GitHubWorkflowError("GitHub App credentials not configured"),
        ):
            response = client.post("/datamanager/add-data/test-id/confirm-async")
        assert response.status_code == 200
        assert b"govuk-error-summary" in response.data
