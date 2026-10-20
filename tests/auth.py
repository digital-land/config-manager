"""Issue real signed identities for tests instead of trusting session profiles."""

from flask import current_app, session

from application.blueprints.auth.auth_token import issue_auth_token


def user_token(user):
    with current_app.test_request_context():
        issue_auth_token(
            subject=user.get("id", 123),
            username=user.get("login", user.get("name", "test-user")),
            role="admin" if user.get("is_admin") else "staff",
        )
        return session["auth_token"]
