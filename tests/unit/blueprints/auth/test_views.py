from http import HTTPStatus
from unittest.mock import Mock, patch

from application.blueprints.auth.views import _is_member_of_admin_team


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
