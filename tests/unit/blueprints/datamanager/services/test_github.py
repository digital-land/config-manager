from unittest.mock import patch, Mock

import pytest

from application.blueprints.datamanager.services.github import (
    GitHubAppAuthError,
    GitHubWorkflowError,
    generate_jwt,
    trigger_add_data_async_workflow,
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
