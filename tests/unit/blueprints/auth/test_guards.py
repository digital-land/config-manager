import time
from datetime import datetime, timezone
from unittest.mock import Mock, patch
from io import BytesIO

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from flask import Flask, g, session
from werkzeug.datastructures import MultiDict

from application.blueprints.datamanager.router import _require_login
from application.blueprints.auth.auth_token import get_principal, has_role, require_auth
from application.blueprints.auth.auth_token import (
    check_auth,
    get_auth_token,
    get_username,
)
from tests.auth import user_token


def test_local_authentication_bypass(app, monkeypatch):
    monkeypatch.setitem(app.config, "ENV", "development")
    monkeypatch.setitem(app.config, "AUTHENTICATION_ON", False)
    monkeypatch.setitem(app.config, "AUTH_JWT_PRIVATE_KEY", None)
    with app.test_request_context("/datamanager/"):
        assert check_auth() is None
        assert has_role("admin")
        assert get_username() == "local-developer"
        assert get_auth_token() is None


@pytest.mark.parametrize("environment", ["production", "test"])
@pytest.mark.parametrize("token_kind", ["missing", "invalid", "expired", "wrong-key"])
def test_session_profile_cannot_bypass_token(app, monkeypatch, token_kind, environment):
    monkeypatch.setitem(app.config, "ENV", environment)
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    monkeypatch.setitem(app.config, "AUTH_JWT_PRIVATE_KEY", key)
    monkeypatch.setitem(app.config, "AUTHENTICATION_ON", False)
    claims = {
        "iss": app.config["AUTH_JWT_ISSUER"],
        "aud": app.config["AUTH_JWT_AUDIENCE"],
        "sub": "123",
        "role": "admin",
        "iat": int(time.time()),
        "exp": int(time.time()) + 900,
    }
    if token_kind == "expired":
        claims["exp"] = 1
    if token_kind == "wrong-key":
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    with app.test_request_context("/datamanager/"):
        session["user"] = {"login": "alice", "is_admin": True}
        session["auth_token_expires"] = time.time() + 900
        if token_kind != "missing":
            session["auth_token"] = (
                "invalid"
                if token_kind == "invalid"
                else jwt.encode(claims, key, algorithm="RS256")
            )
        response = _require_login()
        assert response is not None
        assert response.status_code == 302
        assert "/auth/login?next=" in response.location


@pytest.mark.parametrize("role,status", [("staff", 403), ("admin", 200)])
def test_guard_uses_signed_role_and_actor(app, role, status):
    guarded = Flask(__name__)
    guarded.config.update(app.config)
    guarded.add_url_rule("/login", endpoint="auth.login", view_func=lambda: "login")

    @guarded.get("/admin")
    @require_auth(roles={"admin"})
    def admin():
        return {"actor": g.principal["sub"]}

    client = guarded.test_client()
    with client.session_transaction() as sess:
        sess["user"] = {"id": "forged", "is_admin": True}
        sess["auth_token"] = user_token({"id": 123, "is_admin": role == "admin"})
    response = client.get("/admin")
    assert response.status_code == status
    if status == 200:
        assert response.json == {"actor": "123"}


@pytest.mark.parametrize(
    "field,value",
    [
        ("iss", "untrusted"),
        ("aud", "other-service"),
        ("sub", ""),
        ("exp", None),
        ("iat", None),
        ("role", None),
    ],
)
def test_invalid_identity_claims_do_not_authenticate(app, field, value):
    with app.test_request_context():
        token = user_token({"login": "alice"})
        claims = jwt.decode(token, options={"verify_signature": False})
        claims[field] = value
        session["auth_token"] = jwt.encode(
            claims, app.config["AUTH_JWT_PRIVATE_KEY"], algorithm="RS256"
        )
        assert get_principal() is None


@pytest.mark.parametrize("provider", [None, "another-provider"])
def test_provider_is_optional_in_manage(app, provider):
    with app.test_request_context():
        token = user_token({"login": "alice"})
        claims = jwt.decode(token, options={"verify_signature": False})
        if provider:
            claims["auth_provider"] = provider
        session["auth_token"] = jwt.encode(
            claims, app.config["AUTH_JWT_PRIVATE_KEY"], algorithm="RS256"
        )
        session["user"] = {"is_admin": True}
        assert get_principal()["username"] == "alice"
        assert not has_role("admin")


@pytest.mark.parametrize(
    "url", ["/datamanager/", "/assign-entities", "/process-lock/add-data/toggle"]
)
def test_existing_routes_require_token(client, url):
    with client.session_transaction() as sess:
        sess.pop("auth_token", None)
        sess["user"] = {"login": "alice", "is_admin": True}
    response = client.post(url) if "toggle" in url else client.get(url)
    if "toggle" in url:
        assert response.status_code == 401
        assert b"Sign in to continue" in response.data
    else:
        assert response.status_code == 302
        assert "/auth/login?next=" in response.location


def test_missing_key_fails_closed(client, app, monkeypatch):
    monkeypatch.setitem(app.config, "AUTH_JWT_PRIVATE_KEY", "")
    response = client.get("/assign-entities")
    assert response.status_code == 503


def test_accepted_request_keeps_identity_but_cannot_forward_expired_token(app):
    with app.test_request_context():
        session["auth_token"] = user_token({"login": "alice", "is_admin": True})
        expiry = get_principal()["exp"]
        with patch("jwt.api_jwt.datetime") as clock:
            clock.now.return_value = datetime.fromtimestamp(expiry - 1, timezone.utc)
            assert check_auth() is None
            clock.now.return_value = datetime.fromtimestamp(expiry + 1, timezone.utc)
            assert get_username() == "alice"
            assert has_role("admin")
            assert get_auth_token() is None
    with app.test_request_context():
        assert get_username() == "unknown"
        assert not has_role("admin")


def test_expired_post_keeps_values_for_explicit_retry(client, app):
    with client.session_transaction() as sess:
        claims = jwt.decode(sess["auth_token"], options={"verify_signature": False})
        claims["exp"] = 1
        sess["auth_token"] = jwt.encode(
            claims, app.config["AUTH_JWT_PRIVATE_KEY"], algorithm="RS256"
        )
    with patch(
        "application.blueprints.datamanager.router.handle_add_data_confirm"
    ) as confirm:
        response = client.post(
            "/datamanager/add-data/example/confirm-async",
            data={"github_branch": "review-branch", "override": "true"},
        )
    assert response.status_code == 401
    assert b'action="/datamanager/add-data/example/confirm-async"' in response.data
    assert b'name="github_branch" value="review-branch"' in response.data
    assert b'name="override" value="true"' in response.data
    assert b'target="_blank"' in response.data
    assert response.headers["Cache-Control"] == "no-store"
    confirm.assert_not_called()

    # Login happens in another tab; its callback must never return to the POST URL.
    user = Mock()
    user.json.return_value = {"id": 123, "login": "alice"}
    with patch(
        "application.blueprints.auth.views.oauth.github.authorize_redirect",
        return_value=("OAuth redirect", 302),
    ):
        client.get("/auth/login")
    with patch(
        "application.blueprints.auth.views.oauth.github.authorize_access_token",
        return_value={"access_token": "github-token"},
    ), patch(
        "application.blueprints.auth.views.oauth.github.get", return_value=user
    ), patch(
        "application.blueprints.auth.views.requests.get",
        return_value=Mock(status_code=204),
    ), patch(
        "application.blueprints.auth.views._is_member_of_admin_team", return_value=True
    ), patch(
        "application.blueprints.datamanager.router.handle_add_data_confirm",
        return_value="submitted",
    ) as confirm:
        callback = client.get("/auth/authorize")
        assert callback.location == "/index"
        confirm.assert_not_called()
        retry = client.post(
            "/datamanager/add-data/example/confirm-async",
            data={"github_branch": "review-branch", "override": "true"},
        )
        assert retry.status_code == 200
        confirm.assert_called_once_with(
            "example",
            github_branch="review-branch",
            source_flow="add_data",
            return_url=None,
            override=True,
        )


def test_recovery_preserves_repeated_fields_and_escapes_html(client):
    with client.session_transaction() as sess:
        sess.pop("auth_token", None)
    response = client.post(
        "/datamanager/check-transform/example?view=all",
        data=MultiDict(
            [
                ("retire_endpoints", "first"),
                ("retire_endpoints", "second"),
                ("note", '"><script>alert(1)</script>'),
            ]
        ),
    )
    assert response.status_code == 401
    assert b'action="/datamanager/check-transform/example?view=all"' in response.data
    assert b'name="retire_endpoints" value="first"' in response.data
    assert b'name="retire_endpoints" value="second"' in response.data
    assert b"<script>alert(1)</script>" not in response.data
    assert b"&lt;script&gt;alert(1)&lt;/script&gt;" in response.data
    with client.session_transaction() as sess:
        assert "retire_endpoints" not in sess


def test_recovery_requires_upload_reselection(client):
    with client.session_transaction() as sess:
        sess.pop("auth_token", None)
    response = client.post(
        "/assign-entities/import",
        data={
            "mode": "upload",
            "csv_file": (BytesIO(b"private-file-data"), "data.csv"),
        },
    )
    assert response.status_code == 401
    assert b'enctype="multipart/form-data"' in response.data
    assert b'type="file" name="csv_file"' in response.data
    assert b"select your files again" in response.data
    assert b"private-file-data" not in response.data
