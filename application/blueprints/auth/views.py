from http import HTTPStatus
import logging

import requests
from flask import Blueprint, current_app, flash, redirect, request, session, url_for
from is_safe_url import is_safe_url

from application.extensions import oauth

auth_bp = Blueprint("auth", __name__, url_prefix="/auth")
logger = logging.getLogger(__name__)


def _admin_team_slugs():
    configured_slugs = current_app.config.get(
        "GITHUB_ADMIN_TEAM_SLUGS", "manage-service-admins"
    )
    return [
        slug.strip() for slug in configured_slugs.split(",") if slug and slug.strip()
    ]


def _is_member_of_admin_team(username, headers):
    org = current_app.config.get("GITHUB_ORG", "digital-land")
    github_api_base_url = current_app.config["GITHUB_API_BASE_URL"]
    for team_slug in _admin_team_slugs():
        url = (
            f"{github_api_base_url}/orgs/{org}/teams/{team_slug}"
            f"/memberships/{username}"
        )
        try:
            resp = requests.get(url, headers=headers, timeout=10)
        except requests.RequestException as e:
            logger.warning(
                "Could not check GitHub team membership for %s/%s: %s",
                org,
                team_slug,
                e,
            )
            continue

        if resp.status_code == HTTPStatus.OK:
            if (resp.json() or {}).get("state") == "active":
                return True
            continue
        if resp.status_code not in (HTTPStatus.NOT_FOUND, HTTPStatus.FORBIDDEN):
            logger.warning(
                "Unexpected GitHub team membership response for %s/%s: %s",
                org,
                team_slug,
                resp.status_code,
            )
    return False


@auth_bp.get("/login")
def login():
    session["next"] = _make_next_url_safe(request.args.get("next", None))
    auth_url = url_for("auth.authorize", _external=True)
    return oauth.github.authorize_redirect(auth_url)


@auth_bp.get("/authorize")
def authorize():
    next_url = session.pop("next", None)
    token = oauth.github.authorize_access_token()
    resp = oauth.github.get("user", token=token)
    resp.raise_for_status()
    user_profile = resp.json()
    if user_profile:
        headers = {
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token['access_token']}",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        org = current_app.config.get("GITHUB_ORG", "digital-land")
        # check if user is a member of the configured GitHub org - if they are the members endpoint
        # will return status code 204
        # https://docs.github.com/en/rest/orgs/members?apiVersion=2022-11-28#check-organization-membership-for-a-user
        github_api_base_url = current_app.config["GITHUB_API_BASE_URL"]
        url = f"{github_api_base_url}/orgs/{org}/members/{user_profile['login']}"
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code == HTTPStatus.NO_CONTENT:
            user_profile["is_admin"] = _is_member_of_admin_team(
                user_profile["login"], headers
            )
            session["user"] = user_profile
            next_url = _make_next_url_safe(next_url)
            return redirect(next_url)
        else:
            client_id = current_app.config["GITHUB_CLIENT_ID"]
            client_secret = current_app.config["GITHUB_CLIENT_SECRET"]
            params = {"access_token": token["access_token"]}
            headers = {"X-GitHub-Api-Version": "2022-11-28"}
            resp = requests.delete(
                f"{github_api_base_url}/applications/{client_id}/grant",
                headers=headers,
                params=params,
                auth=(client_id, client_secret),
                timeout=10,
            )
            flash(
                "You must be a member of the digital-land organisation to be logged in"
            )
            return redirect(url_for("base.index"))
    else:
        flash("You must be a member of the digital-land organisation to be logged in")
        return redirect(url_for("base.index"))


@auth_bp.get("/logout")
def logout():
    session.pop("user", None)
    session.pop("next", None)
    return redirect(url_for("base.index"))


def _make_next_url_safe(next_url):
    if next_url is None:
        return url_for("base.index")
    if not is_safe_url(next_url, current_app.config.get("SAFE_URLS", {})):
        return url_for("base.index")
    return next_url
