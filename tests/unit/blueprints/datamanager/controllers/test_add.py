import re
from datetime import datetime
from unittest.mock import patch

from application.blueprints.datamanager.services.github import (
    GitHubAppError,
    GitHubWorkflowError,
)
from application.db.models import EntityClaim, RequestMeta
from application.extensions import db

_CONFIRM = "application.blueprints.datamanager.controllers.confirm"

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
            "application.blueprints.datamanager.controllers.confirm.trigger_add_data_async_workflow",
            return_value={"success": True, "message": "Workflow triggered"},
        ):
            response = client.post(
                "/datamanager/add-data/confirm-success-id/confirm-async"
            )
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
            "application.blueprints.datamanager.controllers.confirm.trigger_add_data_async_workflow",
            return_value={"success": True, "message": "Workflow triggered"},
        ):
            response = client.post(
                "/datamanager/add-data/confirm-assign-linkback/confirm-async",
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
            "application.blueprints.datamanager.controllers.confirm.trigger_add_data_async_workflow",
            return_value={"success": True, "message": "Workflow triggered"},
        ):
            response = client.post(
                "/datamanager/add-data/confirm-assign-default/confirm-async",
                data={"source_flow": "assign_entities"},
            )
        assert response.status_code == 200
        assert b'href="/assign-entities/"' in response.data
        assert b"Assign more entities" in response.data

    def test_confirm_blocks_when_branch_changed(self, client):
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

        with patch(
            f"{_CONFIRM}.config_branch_changed_for_collection", return_value=True
        ), patch(
            f"{_CONFIRM}.fetch_request",
            return_value={"params": {"collection": "conservation-area"}},
        ), patch(f"{_CONFIRM}.trigger_add_data_async_workflow") as trigger:
            response = client.post(
                "/datamanager/add-data/stale-id/confirm-async",
                data={"github_branch": "config-manager-update"},
            )

        assert response.status_code == 200
        # blocked: the workflow must not have been triggered
        trigger.assert_not_called()
        # routed back to the check-results page to re-transform
        assert b"/datamanager/check-results/my-check-id" in response.data

    def test_returns_error_when_workflow_raises(self, client):
        with client.session_transaction() as sess:
            sess["user"] = {"login": "test-user"}
        with patch(
            "application.blueprints.datamanager.controllers.confirm.trigger_add_data_async_workflow",
            side_effect=GitHubWorkflowError("GitHub App credentials not configured"),
        ):
            response = client.post(
                "/datamanager/add-data/confirm-error-id/confirm-async"
            )
        assert response.status_code == 200
        assert b"govuk-error-summary" in response.data

    def test_confirm_does_not_pass_entity_redirects_to_workflow(self, client):
        with client.session_transaction() as sess:
            sess["user"] = {"login": "test-user"}
        with patch(
            "application.blueprints.datamanager.controllers.confirm.trigger_add_data_async_workflow",
            return_value={"success": True, "message": "Workflow triggered"},
        ) as trigger:
            response = client.post(
                "/datamanager/add-data/confirm-redirect-id/confirm-async"
            )

        assert response.status_code == 200
        assert "entity_redirects" not in trigger.call_args.kwargs

    def test_already_submitted_does_not_redispatch(self, client):
        db.session.add(
            RequestMeta(request_id="idem-dispatched", dispatched_at=datetime.utcnow())
        )
        db.session.commit()
        with client.session_transaction() as sess:
            sess["user"] = {"login": "test-user"}
        with patch(f"{_CONFIRM}.trigger_add_data_async_workflow") as trigger:
            response = client.post(
                "/datamanager/add-data/idem-dispatched/confirm-async"
            )
        assert response.status_code == 200
        assert b"already" in response.data.lower()
        trigger.assert_not_called()

    def test_blocks_when_no_baseline_on_shared_branch(self, client):
        db.session.add(RequestMeta(request_id="no-baseline-id"))  # branch_sha is None
        db.session.commit()
        with client.session_transaction() as sess:
            sess["user"] = {"login": "test-user"}
        with patch(
            f"{_CONFIRM}.fetch_request",
            return_value={"params": {"collection": "local-plan"}},
        ), patch(f"{_CONFIRM}.trigger_add_data_async_workflow") as trigger:
            response = client.post(
                "/datamanager/add-data/no-baseline-id/confirm-async",
                data={"github_branch": "config-manager-update"},
            )
        assert response.status_code == 200
        trigger.assert_not_called()

    def test_blocks_on_github_error(self, client):
        db.session.add(RequestMeta(request_id="gh-error-id", branch_sha="base-sha"))
        db.session.commit()
        with client.session_transaction() as sess:
            sess["user"] = {"login": "test-user"}
        with patch(
            f"{_CONFIRM}.fetch_request",
            return_value={"params": {"collection": "local-plan"}},
        ), patch(
            f"{_CONFIRM}.config_branch_changed_for_collection",
            side_effect=GitHubAppError("boom"),
        ), patch(f"{_CONFIRM}.trigger_add_data_async_workflow") as trigger:
            response = client.post(
                "/datamanager/add-data/gh-error-id/confirm-async",
                data={"github_branch": "config-manager-update"},
            )
        assert response.status_code == 200
        trigger.assert_not_called()

    def test_entity_clash_blocks_then_admin_override_dispatches(self, client):
        db.session.add(RequestMeta(request_id="clash-id", branch_sha="base-sha"))
        db.session.add(
            EntityClaim(
                collection="local-plan",
                entity=5111260,
                branch="config-manager-update",
                request_id="other-req",
            )
        )
        db.session.commit()
        resp = {
            "params": {"collection": "local-plan"},
            "response": {
                "data": {
                    "pipeline-summary": {"new-entities": [{"entity": "5111260"}]}
                }
            },
        }

        # non-admin: blocked with the clashing number shown, no dispatch
        with client.session_transaction() as sess:
            sess["user"] = {"login": "user", "is_admin": False}
        with patch(f"{_CONFIRM}.fetch_request", return_value=resp), patch(
            f"{_CONFIRM}.config_branch_changed_for_collection", return_value=False
        ), patch(f"{_CONFIRM}.trigger_add_data_async_workflow") as trigger:
            response = client.post(
                "/datamanager/add-data/clash-id/confirm-async",
                data={"github_branch": "config-manager-update"},
            )
        assert response.status_code == 200
        assert b"5111260" in response.data
        trigger.assert_not_called()

        # admin override: dispatches
        with client.session_transaction() as sess:
            sess["user"] = {"login": "admin", "is_admin": True}
        with patch(f"{_CONFIRM}.fetch_request", return_value=resp), patch(
            f"{_CONFIRM}.config_branch_changed_for_collection", return_value=False
        ), patch(
            f"{_CONFIRM}.trigger_add_data_async_workflow",
            return_value={"success": True, "message": "ok"},
        ) as trigger:
            response = client.post(
                "/datamanager/add-data/clash-id/confirm-async",
                data={
                    "github_branch": "config-manager-update",
                    "override": "true",
                },
            )
        assert response.status_code == 200
        trigger.assert_called_once()

    def test_non_admin_override_flag_is_ignored(self, client):
        db.session.add(RequestMeta(request_id="clash-nonadmin", branch_sha="base-sha"))
        db.session.add(
            EntityClaim(
                collection="local-plan",
                entity=5111300,
                branch="config-manager-update",
                request_id="other-req-2",
            )
        )
        db.session.commit()
        resp = {
            "params": {"collection": "local-plan"},
            "response": {
                "data": {
                    "pipeline-summary": {"new-entities": [{"entity": "5111300"}]}
                }
            },
        }
        # a non-admin sending override=true is still blocked (enforced server-side)
        with client.session_transaction() as sess:
            sess["user"] = {"login": "user", "is_admin": False}
        with patch(f"{_CONFIRM}.fetch_request", return_value=resp), patch(
            f"{_CONFIRM}.config_branch_changed_for_collection", return_value=False
        ), patch(f"{_CONFIRM}.trigger_add_data_async_workflow") as trigger:
            response = client.post(
                "/datamanager/add-data/clash-nonadmin/confirm-async",
                data={
                    "github_branch": "config-manager-update",
                    "override": "true",
                },
            )
        assert response.status_code == 200
        assert b"5111300" in response.data
        trigger.assert_not_called()
