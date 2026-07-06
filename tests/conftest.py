from unittest.mock import patch

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
