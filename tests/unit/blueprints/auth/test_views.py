from http import HTTPStatus
from unittest.mock import Mock, patch

from application.blueprints.auth.views import _is_member_of_admin_team


def test_is_member_of_admin_team_returns_true_for_active_membership(app):
    app.config["GITHUB_ORG"] = "digital-land"
    app.config["GITHUB_ADMIN_TEAM_SLUGS"] = "manage-service-admins"
    response = Mock(status_code=HTTPStatus.OK)
    response.json.return_value = {"state": "active"}

    with app.app_context(), patch(
        "application.blueprints.auth.views.requests.get", return_value=response
    ) as get:
        is_admin = _is_member_of_admin_team("gibahjoe", {"Authorization": "Bearer t"})

    assert is_admin is True
    get.assert_called_once_with(
        "https://api.github.com/orgs/digital-land/teams/manage-service-admins/memberships/gibahjoe",
        headers={"Authorization": "Bearer t"},
        timeout=10,
    )


def test_is_member_of_admin_team_returns_false_when_not_a_member(app):
    app.config["GITHUB_ORG"] = "digital-land"
    app.config["GITHUB_ADMIN_TEAM_SLUGS"] = "manage-service-admins"
    response = Mock(status_code=HTTPStatus.NOT_FOUND)

    with app.app_context(), patch(
        "application.blueprints.auth.views.requests.get", return_value=response
    ):
        is_admin = _is_member_of_admin_team("someone", {})

    assert is_admin is False
