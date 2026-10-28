import base64
import importlib

from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
)

from config.config import Config


def test_wsgi_defaults_to_production_config(app, monkeypatch):
    monkeypatch.delenv("FLASK_CONFIG", raising=False)
    monkeypatch.setattr(Config, "SQLALCHEMY_DATABASE_URI", "sqlite:///:memory:")
    monkeypatch.setattr(Config, "SECRET_KEY", "testing")
    monkeypatch.setattr(Config, "SENTRY_ENABLED", False)
    monkeypatch.setattr(
        Config,
        "AUTH_JWT_PRIVATE_KEY",
        base64.b64encode(
            app.config["AUTH_JWT_PRIVATE_KEY"].private_bytes(
                Encoding.PEM, PrivateFormat.PKCS8, NoEncryption()
            )
        ).decode(),
    )
    from application import wsgi

    production_app = importlib.reload(wsgi).app
    assert production_app.config["ENV"] == "production"
    assert production_app.config["AUTHENTICATION_ON"] is True
    assert production_app.debug is False
    response = production_app.test_client().get("/health")
    assert response.status_code == 200
    assert response.data == b"OK"
    assert production_app.test_client().get("/").status_code == 302


def test_wsgi_allows_explicit_development_config(monkeypatch):
    monkeypatch.setenv("FLASK_CONFIG", "config.DevelopmentConfig")
    from application import wsgi

    development_app = importlib.reload(wsgi).app
    assert development_app.config["ENV"] == "development"
    assert development_app.debug is True
    assert development_app.config["AUTHENTICATION_ON"] is False
