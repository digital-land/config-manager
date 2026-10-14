from unittest.mock import patch, Mock

import pytest

from application.blueprints.datamanager.services.github import (
    MAX_ARTIFACT_ARCHIVE_BYTES,
    GitHubAppAuthError,
    GitHubArtifactError,
    GitHubWorkflowError,
    config_branch_changed_for_collection,
    download_batch_assign_artifact,
    generate_jwt,
    get_latest_batch_assign_artifacts,
    get_branch_head_sha,
    get_config_baseline_sha,
    trigger_add_data_async_workflow,
)


def _with_app_creds(app):
    app.config["GITHUB_APP_ID"] = "app-id"
    app.config["GITHUB_APP_INSTALLATION_ID"] = "install-id"
    app.config["GITHUB_APP_PRIVATE_KEY"] = "key"


@pytest.mark.parametrize("status", [201, 204])
def test_sync_config_branch_merges_main_without_replacing_pending_work(app, status):
    from application.blueprints.datamanager.services import github

    response = Mock(status_code=status)
    response.json.return_value = {"sha": "merged-sha"}
    with app.app_context(), patch.object(
        github, "get_branch_head_sha", return_value="shared-sha"
    ), patch.object(github, "_get_access_token", return_value="token"), patch.object(
        github.requests, "post", return_value=response
    ) as post:
        github.sync_config_branch("shared")
    assert post.call_args.args[0].endswith("/repos/digital-land/config/merges")
    assert post.call_args.kwargs["json"] == {"base": "shared", "head": "main"}


def test_sync_config_branch_leaves_missing_branch_for_lazy_creation(app):
    from application.blueprints.datamanager.services import github

    with app.app_context(), patch.object(
        github, "get_branch_head_sha", return_value=None
    ), patch.object(github.requests, "post") as post:
        github.sync_config_branch("shared")
    post.assert_not_called()


@pytest.mark.parametrize("status", [403, 409, 422, 500])
def test_sync_config_branch_fails_closed(app, status):
    import requests
    from application.blueprints.datamanager.services import github

    response = Mock(status_code=status)
    response.raise_for_status.side_effect = requests.HTTPError(response=response)
    with app.app_context(), patch.object(
        github, "get_branch_head_sha", return_value="shared-sha"
    ), patch.object(github, "_get_access_token", return_value="token"), patch.object(
        github.requests, "post", return_value=response
    ), pytest.raises(
        github.GitHubAppError
    ):
        github.sync_config_branch("shared")


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


class TestGetLatestBatchAssignArtifacts:
    def _artifact(self, name, artifact_id, created_at):
        return {"id": artifact_id, "name": name, "created_at": created_at}

    def test_returns_latest_artifact_for_each_batch_assign_name(self, app):
        responses = []
        for name in (
            "batch-assign-odp-output",
            "batch-assign-mandated-output",
            "batch-assign-single-source-output",
        ):
            response = Mock()
            response.raise_for_status.return_value = None
            response.json.return_value = {
                "artifacts": [
                    self._artifact(name, 1, "2026-08-09T10:00:00Z"),
                    self._artifact(name, 2, "2026-08-10T10:00:00Z"),
                ]
            }
            responses.append(response)

        jwt_p, token_p = _patch_token()
        with app.app_context():
            _with_app_creds(app)
            with jwt_p, token_p, patch(
                "application.blueprints.datamanager.services.github.requests.get",
                side_effect=responses,
            ) as get:
                artifacts = get_latest_batch_assign_artifacts()

        assert [artifact["id"] for artifact in artifacts] == [2, 2, 2]
        assert [call.kwargs["params"]["name"] for call in get.call_args_list] == [
            "batch-assign-odp-output",
            "batch-assign-mandated-output",
            "batch-assign-single-source-output",
        ]

    def test_uses_generated_files_only_when_no_batch_assign_artifacts_exist(self, app):
        empty_responses = []
        for _ in range(3):
            response = Mock()
            response.raise_for_status.return_value = None
            response.json.return_value = {"artifacts": []}
            empty_responses.append(response)

        fallback_response = Mock()
        fallback_response.raise_for_status.return_value = None
        fallback_response.json.return_value = {
            "artifacts": [self._artifact("generated-files", 9, "2026-08-10T11:00:00Z")]
        }

        jwt_p, token_p = _patch_token()
        with app.app_context():
            _with_app_creds(app)
            with jwt_p, token_p, patch(
                "application.blueprints.datamanager.services.github.requests.get",
                side_effect=[*empty_responses, fallback_response],
            ) as get:
                artifacts = get_latest_batch_assign_artifacts()

        assert [artifact["name"] for artifact in artifacts] == ["generated-files"]
        assert get.call_args_list[-1].kwargs["params"]["name"] == "generated-files"


class TestDownloadBatchAssignArtifact:
    def _metadata_response(self, **overrides):
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = {
            "name": "batch-assign-odp-output",
            "expired": False,
            **overrides,
        }
        return response

    def test_uses_metadata_size_before_streaming_download(self, app):
        metadata_response = self._metadata_response(size_in_bytes=1024)
        download_response = Mock()
        download_response.__enter__ = Mock(return_value=download_response)
        download_response.__exit__ = Mock(return_value=False)
        download_response.raise_for_status.return_value = None
        download_response.iter_content.return_value = [b"zip", b"-contents"]

        jwt_p, token_p = _patch_token()
        with app.app_context():
            _with_app_creds(app)
            with jwt_p, token_p, patch(
                "application.blueprints.datamanager.services.github.requests.get",
                side_effect=[metadata_response, download_response],
            ) as get:
                result = download_batch_assign_artifact(123)

        assert result == b"zip-contents"
        assert get.call_count == 2
        assert get.call_args_list[1].kwargs["allow_redirects"] is True
        assert get.call_args_list[1].args[0].endswith("/actions/artifacts/123/zip")

    def test_rejects_archive_that_is_not_smaller_than_20_mb(self, app):
        metadata_response = self._metadata_response(
            size_in_bytes=MAX_ARTIFACT_ARCHIVE_BYTES
        )

        jwt_p, token_p = _patch_token()
        with app.app_context():
            _with_app_creds(app)
            with jwt_p, token_p, patch(
                "application.blueprints.datamanager.services.github.requests.get",
                return_value=metadata_response,
            ) as get:
                with pytest.raises(GitHubArtifactError, match="20 MB or larger"):
                    download_batch_assign_artifact(123)

        get.assert_called_once()

    def test_rejects_stream_that_exceeds_its_reported_metadata_size(self, app):
        metadata_response = self._metadata_response(size_in_bytes=1)
        download_response = Mock()
        download_response.__enter__ = Mock(return_value=download_response)
        download_response.__exit__ = Mock(return_value=False)
        download_response.raise_for_status.return_value = None
        download_response.iter_content.return_value = [
            b"x" * MAX_ARTIFACT_ARCHIVE_BYTES
        ]

        jwt_p, token_p = _patch_token()
        with app.app_context():
            _with_app_creds(app)
            with jwt_p, token_p, patch(
                "application.blueprints.datamanager.services.github.requests.get",
                side_effect=[metadata_response, download_response],
            ):
                with pytest.raises(GitHubArtifactError, match="20 MB or larger"):
                    download_batch_assign_artifact(123)

    def test_rejects_expired_artifact(self, app):
        metadata_response = self._metadata_response(expired=True)
        jwt_p, token_p = _patch_token()
        with app.app_context():
            _with_app_creds(app)
            with jwt_p, token_p, patch(
                "application.blueprints.datamanager.services.github.requests.get",
                return_value=metadata_response,
            ):
                with pytest.raises(GitHubArtifactError, match="expired"):
                    download_batch_assign_artifact(123)

    def test_rejects_artifact_with_an_unapproved_name(self, app):
        metadata_response = self._metadata_response(name="unrelated-output")
        jwt_p, token_p = _patch_token()
        with app.app_context():
            _with_app_creds(app)
            with jwt_p, token_p, patch(
                "application.blueprints.datamanager.services.github.requests.get",
                return_value=metadata_response,
            ):
                with pytest.raises(GitHubArtifactError, match="not a batch assign"):
                    download_batch_assign_artifact(123)


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
