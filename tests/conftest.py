import pytest

from application.extensions import db as _db
from application.factory import create_app


@pytest.fixture(scope="session")
def app():
    app = create_app("config.TestConfig")
    with app.app_context():
        _db.create_all()
    return app


@pytest.fixture(scope="session")
def client(app):
    with app.test_client() as client:
        with app.app_context():
            yield client
