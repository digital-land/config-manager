from unittest.mock import patch

from application.blueprints.datamanager.services.github import GitHubWorkflowError
from application.db.models import RequestMeta
from application.extensions import db

PENDING_ADD_DATA_RESULT = {
    "status": "PENDING",
    "response": None,
    "params": {"dataset": "brownfield-land"},
}


class TestEntitiesPreviewRoute:
    def test_renders_loading_template_when_pending(self, client):
        with patch(
            "application.blueprints.datamanager.router.fetch_request",
            return_value=PENDING_ADD_DATA_RESULT,
        ):
            response = client.get("/datamanager/add-data/test-id/entities")
        assert response.status_code == 200
        assert b"Preparing entities preview" in response.data

    def test_renders_old_entity_redirect_table(self, client):
        db.session.add(
            RequestMeta(
                request_id="test-id",
                entity_redirects=(
                    '[{"old_entity":"100","entity":"200",'
                    '"dataset":"conservation-area"}]'
                ),
            )
        )
        db.session.commit()
        result = {
            "status": "COMPLETE",
            "params": {"dataset": "conservation-area", "authoritative": False},
            "response": {
                "data": {
                    "pipeline-summary": {"new-in-resource": 0},
                    "endpoint-summary": {},
                    "source-summary": {},
                }
            },
        }
        with patch(
            "application.blueprints.datamanager.router.fetch_request",
            return_value=result,
        ):
            response = client.get("/datamanager/add-data/test-id/entities")

        assert response.status_code == 200
        assert b"old-entity.csv" in response.data
        assert b"100" in response.data
        assert b"301" in response.data
        assert b"200" in response.data


class TestAddDataConfirmRoute:
    def test_renders_success_when_workflow_triggered(self, client):
        with client.session_transaction() as sess:
            sess["user"] = {"login": "test-user"}
        with patch(
            "application.blueprints.datamanager.controllers.preview.trigger_add_data_async_workflow",
            return_value={"success": True, "message": "Workflow triggered"},
        ):
            response = client.post("/datamanager/add-data/test-id/confirm-async")
        assert response.status_code == 200
        assert (
            b"triggered" in response.data.lower() or b"success" in response.data.lower()
        )
        assert b'href="/datamanager/"' in response.data
        assert b"Add more data" in response.data

    def test_assign_entities_success_links_back_to_assign_entities(self, client):
        with client.session_transaction() as sess:
            sess["user"] = {"login": "test-user"}
        with patch(
            "application.blueprints.datamanager.controllers.preview.trigger_add_data_async_workflow",
            return_value={"success": True, "message": "Workflow triggered"},
        ):
            response = client.post(
                "/datamanager/add-data/test-id/confirm-async",
                data={
                    "source_flow": "assign_entities",
                    "return_url": "/assign-entities/resources",
                },
            )
        assert response.status_code == 200
        assert b'href="/assign-entities/resources"' in response.data
        assert b"Assign more entities" in response.data

    def test_assign_entities_success_defaults_back_to_start_page(self, client):
        with client.session_transaction() as sess:
            sess["user"] = {"login": "test-user"}
        with patch(
            "application.blueprints.datamanager.controllers.preview.trigger_add_data_async_workflow",
            return_value={"success": True, "message": "Workflow triggered"},
        ):
            response = client.post(
                "/datamanager/add-data/test-id/confirm-async",
                data={"source_flow": "assign_entities"},
            )
        assert response.status_code == 200
        assert b'href="/assign-entities/"' in response.data
        assert b"Assign more entities" in response.data

    def test_returns_error_when_workflow_raises(self, client):
        with client.session_transaction() as sess:
            sess["user"] = {"login": "test-user"}
        with patch(
            "application.blueprints.datamanager.controllers.preview.trigger_add_data_async_workflow",
            side_effect=GitHubWorkflowError("GitHub App credentials not configured"),
        ):
            response = client.post("/datamanager/add-data/test-id/confirm-async")
        assert response.status_code == 200
        assert b"govuk-error-summary" in response.data

    def test_confirm_passes_entity_redirects_to_workflow(self, client):
        db.session.add(
            RequestMeta(
                request_id="confirm-redirect-id",
                entity_redirects=(
                    '[{"old_entity":"100","entity":"200",'
                    '"dataset":"conservation-area"}]'
                ),
            )
        )
        db.session.commit()
        with client.session_transaction() as sess:
            sess["user"] = {"login": "test-user"}
        with patch(
            "application.blueprints.datamanager.controllers.preview.trigger_add_data_async_workflow",
            return_value={"success": True, "message": "Workflow triggered"},
        ) as trigger:
            response = client.post(
                "/datamanager/add-data/confirm-redirect-id/confirm-async"
            )

        assert response.status_code == 200
        assert trigger.call_args.kwargs["entity_redirects"] == [
            {
                "old_entity": "100",
                "entity": "200",
                "dataset": "conservation-area",
            }
        ]
