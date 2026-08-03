import re
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
        result = {
            "status": "COMPLETE",
            "params": {
                "dataset": "conservation-area",
                "authoritative": False,
                "resource": "resource-a",
                "excluded_references": ["not-selected", "not-selected", ""],
            },
            "response": {
                "data": {
                    "pipeline-summary": {
                        "new-in-resource": 0,
                        "old-entity": [
                            {
                                "old-entity": "100",
                                "status": "301",
                                "entity": "200",
                            },
                            {
                                "old-entity": "101",
                                "status": "301",
                                "entity": "201",
                            },
                            {
                                "old-entity": "102",
                                "status": "410",
                                "entity": "202",
                            },
                        ],
                        "new-entities": [
                            {"entity": "200", "reference": "new-ref"},
                            {"entity": "201", "reference": "other-ref"},
                        ],
                    },
                    "endpoint-summary": {},
                    "source-summary": {},
                }
            },
        }
        db.session.add(RequestMeta(request_id="test-id", source_flow="assign_entities"))
        db.session.commit()
        with patch(
            "application.blueprints.datamanager.router.fetch_request",
            return_value=result,
        ):
            response = client.get("/datamanager/add-data/test-id/entities")

        assert response.status_code == 200
        assert b"old-entity.csv" in response.data
        assert b'<h3 class="govuk-heading-s">Redirects</h3>' not in response.data
        assert b'<h3 class="govuk-heading-s">Retirements</h3>' not in response.data
        assert b"100" in response.data
        assert b"101" in response.data
        assert b"301" in response.data
        assert b"200" in response.data
        assert b"Number of redirects" in response.data
        assert re.search(
            rb"Number of redirects.*?<dd class=\"govuk-summary-list__value\">2</dd>",
            response.data,
            re.S,
        )
        assert re.search(
            rb"Number of retirements.*?<dd class=\"govuk-summary-list__value\">1</dd>",
            response.data,
            re.S,
        )
        assert b"Rows that will create new entities" in response.data
        assert b'<dd class="govuk-summary-list__value">2</dd>' in response.data
        assert b"Rows that will" in response.data
        assert b"NOT</span> create new entities" in response.data
        assert re.search(
            rb"Rows that will.*?NOT</span> create new entities.*?<dd class=\"govuk-summary-list__value\">1</dd>",
            response.data,
            re.S,
        )


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

    def test_assign_entities_success_returns_to_flagged_resources(self, client):
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
                    "return_url": "/assign-entities/",
                },
            )
        assert response.status_code == 200
        assert b'href="/assign-entities/resources"' in response.data
        assert b"Assign more entities" in response.data

    def test_confirm_waits_for_workflow_then_blocks_when_branch_changed(self, client):
        db.session.add(
            RequestMeta(
                request_id="stale-id",
                branch_sha="base-sha",
                check_request_id="my-check-id",
            )
        )
        db.session.commit()
        with client.session_transaction() as sess:
            sess["user"] = {"login": "test-user"}

        calls = []
        with patch(
            "application.blueprints.datamanager.controllers.preview."
            "wait_for_add_data_workflow_idle",
            side_effect=lambda *a, **k: calls.append("wait"),
        ), patch(
            "application.blueprints.datamanager.controllers.preview."
            "config_branch_changed_for_collection",
            side_effect=lambda *a, **k: calls.append("compare") or True,
        ), patch(
            "application.blueprints.datamanager.controllers.preview.fetch_request",
            return_value={"params": {"collection": "conservation-area"}},
        ), patch(
            "application.blueprints.datamanager.controllers.preview."
            "trigger_add_data_async_workflow",
        ) as trigger:
            response = client.post(
                "/datamanager/add-data/stale-id/confirm-async",
                data={"github_branch": "config-manager-update"},
            )

        assert response.status_code == 200
        # workflow-idle wait must run BEFORE the branch comparison
        assert calls == ["wait", "compare"]
        # blocked: the workflow must not have been triggered
        trigger.assert_not_called()
        # routed back to the check-results page to re-transform
        assert b"/datamanager/check-results/my-check-id" in response.data

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

    def test_confirm_does_not_pass_entity_redirects_to_workflow(self, client):
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
        assert "entity_redirects" not in trigger.call_args.kwargs
