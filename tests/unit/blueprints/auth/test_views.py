from http import HTTPStatus
from unittest.mock import Mock, patch

import jwt
import pytest
from flask import session
from cryptography.hazmat.primitives.asymmetric import rsa

from application.blueprints.auth.views import _is_member_of_admin_team


@pytest.mark.parametrize("is_admin,role", [(True, "admin"), (False, "staff")])
def test_authorize_issues_auth_token_and_logout_clears_it(
    client, app, monkeypatch, is_admin, role
):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    monkeypatch.setitem(app.config, "AUTH_JWT_PRIVATE_KEY", key)
    monkeypatch.setitem(app.config, "AUTH_JWT_ISSUER", "manage-test")
    monkeypatch.setitem(app.config, "AUTH_JWT_AUDIENCE", "async-test")
    user = Mock()
    user.json.return_value = {"id": 123, "login": "alice"}
    with patch(
        "application.blueprints.auth.views.oauth.github.authorize_access_token",
        return_value={"access_token": "github-token"},
    ), patch(
        "application.blueprints.auth.views.oauth.github.get", return_value=user
    ), patch(
        "application.blueprints.auth.views.requests.get",
        return_value=Mock(status_code=204),
    ), patch(
        "application.blueprints.auth.views._is_member_of_admin_team",
        return_value=is_admin,
    ):
        assert client.get("/auth/authorize").status_code == 302
    with client.session_transaction() as session:
        assert "auth_token" in session
        claims = jwt.decode(
            session["auth_token"],
            key.public_key(),
            algorithms=["RS256"],
            issuer="manage-test",
            audience="async-test",
        )
    assert claims["sub"] == "123"
    assert claims["auth_provider"] == "github"
    assert claims["role"] == role
    assert claims["exp"] - claims["iat"] == 900
    client.get("/auth/logout")
    with client.session_transaction() as session:
        assert "auth_token" not in session


def test_expired_auth_token_requires_login(app, monkeypatch):
    from application.blueprints.datamanager.router import _require_login

    monkeypatch.setitem(app.config, "AUTHENTICATION_ON", True)
    with app.test_request_context("/datamanager/"):
        session["user"] = {"login": "alice"}
        session["auth_token"] = "expired"
        session["auth_token_expires"] = 1
        response = _require_login()
    assert response.status_code == 302
    assert "/auth/login?next=" in response.location


def test_is_member_of_admin_team_returns_true_for_active_membership(app):
    app.config["GITHUB_ORG"] = "digital-land"
    app.config["GITHUB_ADMIN_TEAM_SLUGS"] = "manage-service-admins"
    app.config["GITHUB_API_BASE_URL"] = "https://api.github.com"
    response = Mock(status_code=HTTPStatus.OK)
    response.json.return_value = {"state": "active"}

    with app.app_context(), patch(
        "application.blueprints.auth.views.requests.get", return_value=response
    ) as get:
        is_admin = _is_member_of_admin_team("gibahjoe", {"Authorization": "Bearer t"})

    assert is_admin is True
    get.assert_called_once_with(
        f"{app.config['GITHUB_API_BASE_URL']}/orgs/digital-land/teams/manage-service-admins/memberships/gibahjoe",
        headers={"Authorization": "Bearer t"},
        timeout=10,
    )


def test_is_member_of_admin_team_returns_false_when_not_a_member(app):
    app.config["GITHUB_ORG"] = "digital-land"
    app.config["GITHUB_ADMIN_TEAM_SLUGS"] = "manage-service-admins"
    app.config["GITHUB_API_BASE_URL"] = "https://api.github.com"
    response = Mock(status_code=HTTPStatus.NOT_FOUND)

    with app.app_context(), patch(
        "application.blueprints.auth.views.requests.get", return_value=response
    ):
        is_admin = _is_member_of_admin_team("someone", {})

    assert is_admin is False


def test_is_member_of_admin_team_checks_later_team_after_pending_membership(app):
    app.config["GITHUB_ORG"] = "digital-land"
    app.config["GITHUB_ADMIN_TEAM_SLUGS"] = "team-one,team-two"
    app.config["GITHUB_API_BASE_URL"] = "https://api.github.com"
    pending_response = Mock(status_code=HTTPStatus.OK)
    pending_response.json.return_value = {"state": "pending"}
    active_response = Mock(status_code=HTTPStatus.OK)
    active_response.json.return_value = {"state": "active"}

    with app.app_context(), patch(
        "application.blueprints.auth.views.requests.get",
        side_effect=[pending_response, active_response],
    ) as get:
        is_admin = _is_member_of_admin_team("gibahjoe", {"Authorization": "Bearer t"})

    assert is_admin is True
    assert get.call_count == 2
    assert get.call_args_list[0].args[0] == (
        f"{app.config['GITHUB_API_BASE_URL']}/orgs/digital-land/teams/team-one/memberships/gibahjoe"
    )
    assert get.call_args_list[1].args[0] == (
        f"{app.config['GITHUB_API_BASE_URL']}/orgs/digital-land/teams/team-two/memberships/gibahjoe"
    )


def test_authorize_checks_configured_org_membership_with_timeout(client, app):
    app.config["GITHUB_ORG"] = "custom-org"
    app.config["GITHUB_API_BASE_URL"] = "https://api.github.com"
    token = {"access_token": "token"}
    user_response = Mock()
    user_response.json.return_value = {"id": 123, "login": "gibahjoe"}
    membership_response = Mock(status_code=HTTPStatus.NO_CONTENT)

    with patch(
        "application.blueprints.auth.views.oauth.github.authorize_access_token",
        return_value=token,
    ), patch(
        "application.blueprints.auth.views.oauth.github.get",
        return_value=user_response,
    ), patch(
        "application.blueprints.auth.views.requests.get",
        return_value=membership_response,
    ) as get, patch(
        "application.blueprints.auth.views._is_member_of_admin_team",
        return_value=False,
    ):
        response = client.get("/auth/authorize")

    assert response.status_code == 302
    get.assert_called_once_with(
        f"{app.config['GITHUB_API_BASE_URL']}/orgs/custom-org/members/gibahjoe",
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": "Bearer token",
            "X-GitHub-Api-Version": "2022-11-28",
        },
        timeout=10,
    )


def test_authorize_revokes_grant_with_timeout_when_not_org_member(client, app):
    app.config["GITHUB_ORG"] = "custom-org"
    app.config["GITHUB_API_BASE_URL"] = "https://api.github.com"
    app.config["GITHUB_CLIENT_ID"] = "client-id"
    app.config["GITHUB_CLIENT_SECRET"] = "client-secret"
    token = {"access_token": "token"}
    user_response = Mock()
    user_response.json.return_value = {"login": "gibahjoe"}
    membership_response = Mock(status_code=HTTPStatus.NOT_FOUND)
    revoke_response = Mock()

    with patch(
        "application.blueprints.auth.views.oauth.github.authorize_access_token",
        return_value=token,
    ), patch(
        "application.blueprints.auth.views.oauth.github.get",
        return_value=user_response,
    ), patch(
        "application.blueprints.auth.views.requests.get",
        return_value=membership_response,
    ), patch(
        "application.blueprints.auth.views.requests.delete",
        return_value=revoke_response,
    ) as delete:
        response = client.get("/auth/authorize")

    assert response.status_code == 302
    delete.assert_called_once_with(
        f"{app.config['GITHUB_API_BASE_URL']}/applications/client-id/grant",
        headers={"X-GitHub-Api-Version": "2022-11-28"},
        params={"access_token": "token"},
        auth=("client-id", "client-secret"),
        timeout=10,
    )
