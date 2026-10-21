"""Issue short-lived tokens for API calls from an authenticated Manage session."""

import time

import jwt
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from flask import abort, current_app, has_request_context, session


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
    return jwt.encode(claims, _signing_key(), algorithm="RS256")


def get_auth_token():
    """Mint a fresh token for this API call; never use a cached browser JWT."""
    if not has_request_context():
        return None
    user = session.get("user")
    if not user or not user.get("id") or not user.get("login"):
        return None
    return issue_auth_token(
        subject=user["id"],
        username=user["login"],
        role="admin" if user.get("is_admin") else "staff",
        auth_provider="github",
    )
