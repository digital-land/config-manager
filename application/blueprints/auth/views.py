import requests
from flask import Blueprint, current_app, redirect, request, session, url_for
from is_safe_url import is_safe_url

from application.extensions import oauth

auth_bp = Blueprint("auth", __name__, url_prefix="/auth")


@auth_bp.get("/login")
def login():
    session["next"] = _make_next_url_safe(request.args.get("next", None))
    auth_url = url_for("auth.authorize", _external=True)
    return oauth.github.authorize_redirect(auth_url)


@auth_bp.get("/authorize")
def authorize():
    next_url = session.pop("next", None)
    token = oauth.github.authorize_access_token()
    resp = oauth.github.get("user/orgs", token=token)
    resp.raise_for_status()
    orgs = resp.json()
    is_in_organisation = any((org["login"] == "digital-land" for org in orgs))

    if is_in_organisation:
        resp = oauth.github.get("user", token=token)
        resp.raise_for_status()
        user_profile = resp.json()
        if user_profile:
            session["user"] = user_profile
        next_url = _make_next_url_safe(next_url)
        return redirect(next_url)
    else:
        client_id = current_app.config["GITHUB_CLIENT_ID"]
        client_secret = current_app.config["GITHUB_CLIENT_SECRET"]
        headers = {"Accept": "application/vnd.github.v3+json"}
        params = {"access_token": token["access_token"]}
        requests.delete(
            f"https://api.github.com/applications/{client_id}/grant",
            json=params,
            headers=headers,
            auth=(client_id, client_secret),
        )
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
