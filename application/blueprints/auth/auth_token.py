"""Signed authentication tokens for Manage users, separate from GitHub tokens."""

import time
from functools import wraps

import jwt
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from flask import (
    abort,
    current_app,
    g,
    has_request_context,
    redirect,
    render_template,
    request,
    session,
    url_for,
)


def _signing_key():
    key = current_app.config.get("AUTH_JWT_PRIVATE_KEY")
    if not key:
        abort(503, description="Authentication is not configured")
    try:
        return (
            load_pem_private_key(key.encode(), password=None)
            if isinstance(key, str)
            else key
        )
    except (ValueError, TypeError):
        abort(503, description="Authentication is not configured")


def issue_auth_token(*, subject, role, username, auth_provider=None):
    session.pop("auth_token", None)
    session.pop("auth_token_expires", None)
    issued = int(time.time())
    claims = {
        "iss": current_app.config["AUTH_JWT_ISSUER"],
        "aud": current_app.config["AUTH_JWT_AUDIENCE"],
        "sub": str(subject),
        "username": username,
        "role": role,
        "iat": issued,
        "exp": issued + 900,
    }
    if auth_provider is not None:
        claims["auth_provider"] = auth_provider
    session["auth_token"] = jwt.encode(
        claims,
        _signing_key(),
        algorithm="RS256",
    )


def get_principal():
    if not has_request_context() or not session.get("auth_token"):
        return None
    try:
        claims = jwt.decode(
            session["auth_token"],
            _signing_key().public_key(),
            algorithms=["RS256"],
            issuer=current_app.config["AUTH_JWT_ISSUER"],
            audience=current_app.config["AUTH_JWT_AUDIENCE"],
            options={"require": ["iss", "aud", "sub", "iat", "exp", "role"]},
        )
    except (jwt.InvalidTokenError, ValueError, TypeError):
        return None
    if not claims["sub"] or not isinstance(claims["role"], str):
        return None
    return claims


def get_auth_token():
    if get_principal():
        return session.get("auth_token")
    return None


def get_request_principal():
    """Keep the accepted identity stable within this request, never across requests."""
    if not has_request_context():
        return None
    if (
        current_app.config.get("ENV") == "development"
        and current_app.config.get("AUTHENTICATION_ON", True) is False
    ):
        return {
            "sub": "local-developer",
            "username": "local-developer",
            "role": "admin",
        }
    return request.environ.get("manage.auth_principal") or get_principal()


def has_role(role):
    return (get_request_principal() or {}).get("role") == role


def get_username():
    principal = get_request_principal() or {}
    return principal.get("username") or principal.get("sub", "unknown")


def check_auth(roles=("staff", "admin")):
    principal = get_request_principal()
    if principal is None:
        if request.method == "POST":
            # Keep form values in the response, not the size-limited session cookie.
            # The user signs in separately and explicitly submits the form again.
            return (
                render_template(
                    "auth/sign-in-again.html",
                    action=request.full_path if request.query_string else request.path,
                    fields=list(request.form.items(multi=True)),
                    uploads=[
                        (name, len(files))
                        for name, files in request.files.lists()
                        if any(file.filename for file in files)
                    ],
                ),
                401,
                {"Cache-Control": "no-store"},
            )
        return redirect(url_for("auth.login", next=request.url))
    if principal["role"] not in roles:
        return "Forbidden", 403
    g.principal = principal
    request.environ["manage.auth_principal"] = principal


def require_auth(*, roles=("staff", "admin")):
    def decorate(handler):
        @wraps(handler)
        def wrapped(*args, **kwargs):
            denied = check_auth(roles)
            return denied if denied is not None else handler(*args, **kwargs)

        return wrapped

    return decorate
