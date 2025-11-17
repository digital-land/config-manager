import pytest

from application.factory import create_app


@pytest.fixture(scope="session")
def app():
    app = create_app("config.TestConfig")
    return app


@pytest.fixture(scope="session")
def client(app):
    with app.test_client() as client:
        with app.app_context():
            yield client
