from unittest.mock import patch, Mock

import pytest

from application.blueprints.datamanager.services.github import (
    GitHubAppAuthError,
    GitHubWorkflowError,
    add_data_workflow_running,
    config_branch_changed_for_collection,
    generate_jwt,
    get_branch_head_sha,
    get_config_baseline_sha,
    trigger_add_data_async_workflow,
    wait_for_add_data_workflow_idle,
)


def _with_app_creds(app):
    app.config["GITHUB_APP_ID"] = "app-id"
    app.config["GITHUB_APP_INSTALLATION_ID"] = "install-id"
    app.config["GITHUB_APP_PRIVATE_KEY"] = "key"


def _patch_token():
    return (
        patch(
            "application.blueprints.datamanager.services.github.generate_jwt",
            return_value="jwt-token",
        ),
        patch(
            "application.blueprints.datamanager.services.github.get_installation_token",
            return_value="access-token",
        ),
    )


class TestGenerateJwt:
    def test_raises_on_invalid_key(self):
        with pytest.raises(GitHubAppAuthError):
            generate_jwt(app_id="123", private_key="not-a-valid-key")


class TestTriggerAddDataAsyncWorkflow:
    def test_raises_when_credentials_missing(self, app):
        with app.app_context():
            app.config["GITHUB_APP_ID"] = None
            app.config["GITHUB_APP_INSTALLATION_ID"] = None
            app.config["GITHUB_APP_PRIVATE_KEY"] = None
            with pytest.raises(GitHubWorkflowError, match="not configured"):
                trigger_add_data_async_workflow("request-123")

    def test_returns_success_on_204(self, app):
        mock_dispatch = Mock()
        mock_dispatch.status_code = 204

        with app.app_context():
            app.config["GITHUB_APP_ID"] = "app-id"
            app.config["GITHUB_APP_INSTALLATION_ID"] = "install-id"
            app.config["GITHUB_APP_PRIVATE_KEY"] = "key"
            with patch(
                "application.blueprints.datamanager.services.github.generate_jwt",
                return_value="jwt-token",
            ):
                with patch(
                    "application.blueprints.datamanager.services.github.get_installation_token",
                    return_value="access-token",
                ):
                    with patch(
                        "application.blueprints.datamanager.services.github.requests.post",
                        return_value=mock_dispatch,
                    ):
                        result = trigger_add_data_async_workflow("request-123")

        assert result["success"] is True
        assert result["status_code"] == 204

    def test_returns_failure_on_non_204(self, app):
        mock_dispatch = Mock()
        mock_dispatch.status_code = 422
        mock_dispatch.text = "Unprocessable Entity"

        with app.app_context():
            app.config["GITHUB_APP_ID"] = "app-id"
            app.config["GITHUB_APP_INSTALLATION_ID"] = "install-id"
            app.config["GITHUB_APP_PRIVATE_KEY"] = "key"
            with patch(
                "application.blueprints.datamanager.services.github.generate_jwt",
                return_value="jwt-token",
            ):
                with patch(
                    "application.blueprints.datamanager.services.github.get_installation_token",
                    return_value="access-token",
                ):
                    with patch(
                        "application.blueprints.datamanager.services.github.requests.post",
                        return_value=mock_dispatch,
                    ):
                        result = trigger_add_data_async_workflow("request-123")

        assert result["success"] is False
        assert result["status_code"] == 422

    def test_does_not_include_entity_redirects_in_payload(self, app):
        mock_dispatch = Mock()
        mock_dispatch.status_code = 204

        with app.app_context():
            app.config["GITHUB_APP_ID"] = "app-id"
            app.config["GITHUB_APP_INSTALLATION_ID"] = "install-id"
            app.config["GITHUB_APP_PRIVATE_KEY"] = "key"
            with patch(
                "application.blueprints.datamanager.services.github.generate_jwt",
                return_value="jwt-token",
            ):
                with patch(
                    "application.blueprints.datamanager.services.github.get_installation_token",
                    return_value="access-token",
                ):
                    with patch(
                        "application.blueprints.datamanager.services.github.requests.post",
                        return_value=mock_dispatch,
                    ) as post:
                        trigger_add_data_async_workflow("request-123")

        payload = post.call_args.kwargs["json"]
        assert "entity_redirects" not in payload["client_payload"]

class TestGetBranchHeadSha:
    def test_returns_sha(self, app):
        resp = Mock()
        resp.status_code = 200
        resp.json.return_value = {"commit": {"sha": "abc123"}}
        jwt_p, token_p = _patch_token()
        with app.app_context():
            _with_app_creds(app)
            with jwt_p, token_p, patch(
                "application.blueprints.datamanager.services.github.requests.get",
                return_value=resp,
            ):
                assert get_branch_head_sha("config-manager-update") == "abc123"

    def test_returns_none_on_404(self, app):
        resp = Mock()
        resp.status_code = 404
        jwt_p, token_p = _patch_token()
        with app.app_context():
            _with_app_creds(app)
            with jwt_p, token_p, patch(
                "application.blueprints.datamanager.services.github.requests.get",
                return_value=resp,
            ):
                assert get_branch_head_sha("missing-branch") is None


class TestAddDataWorkflowRunning:
    def _run(self, app, statuses):
        resp = Mock()
        resp.raise_for_status.return_value = None
        resp.json.return_value = {"workflow_runs": [{"status": s} for s in statuses]}
        jwt_p, token_p = _patch_token()
        with app.app_context():
            _with_app_creds(app)
            with jwt_p, token_p, patch(
                "application.blueprints.datamanager.services.github.requests.get",
                return_value=resp,
            ):
                return add_data_workflow_running()

    def test_true_when_in_progress(self, app):
        assert self._run(app, ["completed", "in_progress"]) is True

    def test_true_when_queued(self, app):
        assert self._run(app, ["queued"]) is True

    def test_false_when_all_completed(self, app):
        assert self._run(app, ["completed", "completed"]) is False

    def test_false_when_no_runs(self, app):
        assert self._run(app, []) is False

    def test_false_on_api_error(self, app):
        import requests as requests_lib

        resp = Mock()
        resp.raise_for_status.side_effect = requests_lib.exceptions.RequestException(
            "boom"
        )
        jwt_p, token_p = _patch_token()
        with app.app_context():
            _with_app_creds(app)
            with jwt_p, token_p, patch(
                "application.blueprints.datamanager.services.github.requests.get",
                return_value=resp,
            ):
                assert add_data_workflow_running() is False


class TestWaitForAddDataWorkflowIdle:
    def test_returns_immediately_when_idle(self, app):
        jwt_p, token_p = _patch_token()
        with app.app_context():
            _with_app_creds(app)
            with jwt_p, token_p, patch(
                "application.blueprints.datamanager.services.github._add_data_workflow_active",
                return_value=False,
            ) as active, patch(
                "application.blueprints.datamanager.services.github.time.sleep"
            ) as sleep:
                assert wait_for_add_data_workflow_idle() is True
        assert active.call_count == 1
        sleep.assert_not_called()

    def test_polls_until_idle(self, app):
        jwt_p, token_p = _patch_token()
        with app.app_context():
            _with_app_creds(app)
            with jwt_p, token_p, patch(
                "application.blueprints.datamanager.services.github._add_data_workflow_active",
                side_effect=[True, True, False],
            ), patch(
                "application.blueprints.datamanager.services.github.time.sleep"
            ) as sleep:
                assert (
                    wait_for_add_data_workflow_idle(timeout=60, poll_interval=5) is True
                )
        assert sleep.call_count == 2

    def test_gives_up_after_timeout(self, app):
        jwt_p, token_p = _patch_token()
        with app.app_context():
            _with_app_creds(app)
            with jwt_p, token_p, patch(
                "application.blueprints.datamanager.services.github._add_data_workflow_active",
                return_value=True,
            ), patch("application.blueprints.datamanager.services.github.time.sleep"):
                assert (
                    wait_for_add_data_workflow_idle(timeout=5, poll_interval=5) is False
                )


class TestGetConfigBaselineSha:
    def test_uses_branch_when_it_exists(self, app):
        with app.app_context():
            _with_app_creds(app)
            with patch(
                "application.blueprints.datamanager.services.github.get_branch_head_sha",
                return_value="branch-sha",
            ) as head:
                assert get_config_baseline_sha("config-manager-update") == "branch-sha"
        head.assert_called_once_with("config-manager-update")

    def test_falls_back_to_main_when_branch_absent(self, app):
        with app.app_context():
            _with_app_creds(app)
            with patch(
                "application.blueprints.datamanager.services.github.get_branch_head_sha",
                side_effect=lambda b: (
                    None if b == "config-manager-update" else "main-sha"
                ),
            ) as head:
                assert get_config_baseline_sha("config-manager-update") == "main-sha"
        assert [c.args[0] for c in head.call_args_list] == [
            "config-manager-update",
            "main",
        ]


class TestConfigBranchChangedForCollection:
    def _run(self, app, json_data, branch_exists=True):
        resp = Mock()
        resp.status_code = 200
        resp.raise_for_status.return_value = None
        resp.json.return_value = json_data
        jwt_p, token_p = _patch_token()
        with app.app_context():
            _with_app_creds(app)
            with jwt_p, token_p, patch(
                "application.blueprints.datamanager.services.github.get_branch_head_sha",
                return_value=("head-sha" if branch_exists else None),
            ), patch(
                "application.blueprints.datamanager.services.github.requests.get",
                return_value=resp,
            ) as get:
                result = config_branch_changed_for_collection(
                    "base-sha", "config-manager-update", "conservation-area"
                )
        return result, get

    def test_identical_is_unchanged(self, app):
        result, _ = self._run(app, {"status": "identical", "files": []})
        assert result is False

    def test_ahead_but_other_collection_is_unchanged(self, app):
        data = {
            "status": "ahead",
            "files": [{"filename": "pipeline/brownfield-land/lookup.csv"}],
        }
        assert self._run(app, data)[0] is False

    def test_ahead_touching_collection_is_changed(self, app):
        data = {
            "status": "ahead",
            "files": [{"filename": "pipeline/conservation-area/lookup.csv"}],
        }
        assert self._run(app, data)[0] is True

    def test_diverged_fails_closed(self, app):
        assert self._run(app, {"status": "diverged", "files": []})[0] is True

    def test_truncated_file_list_fails_closed(self, app):
        data = {
            "status": "ahead",
            "files": [{"filename": "pipeline/other/x.csv"}] * 300,
        }
        assert self._run(app, data)[0] is True

    def test_compares_against_main_when_branch_absent(self, app):
        data = {
            "status": "ahead",
            "files": [{"filename": "pipeline/conservation-area/lookup.csv"}],
        }
        result, get = self._run(app, data, branch_exists=False)
        assert result is True
        assert get.call_args.args[0].endswith("/compare/base-sha...main")

    def test_api_error_fails_closed(self, app):
        import requests as requests_lib

        resp = Mock()
        resp.raise_for_status.side_effect = requests_lib.exceptions.RequestException(
            "boom"
        )
        jwt_p, token_p = _patch_token()
        with app.app_context():
            _with_app_creds(app)
            with jwt_p, token_p, patch(
                "application.blueprints.datamanager.services.github.get_branch_head_sha",
                return_value="head-sha",
            ), patch(
                "application.blueprints.datamanager.services.github.requests.get",
                return_value=resp,
            ):
                assert (
                    config_branch_changed_for_collection(
                        "base-sha", "config-manager-update", "conservation-area"
                    )
                    is True
                )
