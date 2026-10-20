from unittest.mock import patch

import pytest
from cryptography.hazmat.primitives.asymmetric import rsa

from tests.auth import user_token
from application.extensions import db as _db
from application.factory import create_app


@pytest.fixture(scope="session")
def app():
    app = create_app("config.TestConfig")
    app.config["AUTH_JWT_PRIVATE_KEY"] = rsa.generate_private_key(
        public_exponent=65537, key_size=2048
    )
    with app.app_context():
        _db.create_all()
    return app


@pytest.fixture(scope="session")
def client(app):
    with app.test_client() as client:
        with app.app_context():
            yield client


@pytest.fixture(autouse=True)
def authenticated_session(app, client):
    """A logged-in user's token for journeys with a mocked async API."""
    with client.session_transaction() as sess:
        sess["auth_token"] = user_token({"login": "test-user"})
    yield
    with client.session_transaction() as sess:
        sess.pop("auth_token", None)


@pytest.fixture(autouse=True)
def mock_dataset_typology():
    """Prevent get_dataset_typology from making HTTP calls in tests.

    Defaults to '' (non-geography — no map). Tests that exercise geography
    behaviour patch it explicitly to 'geography'.
    """
    with patch(
        "application.blueprints.datamanager.controllers.transform.get_dataset_typology",
        return_value="",
    ):
        yield
