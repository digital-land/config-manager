import base64

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import ec, rsa
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
)

from application.factory import create_app
from config.config import TestConfig


def encoded_key(key):
    return base64.b64encode(
        key.private_bytes(Encoding.PEM, PrivateFormat.PKCS8, NoEncryption())
    ).decode()


@pytest.mark.parametrize(
    "value",
    ["", "not-base64!", base64.b64encode(b"not a key").decode()],
)
def test_authenticated_startup_rejects_invalid_key(value):
    class AuthConfig(TestConfig):
        AUTHENTICATION_ON = True
        AUTH_JWT_PRIVATE_KEY = value

    with pytest.raises(ValueError, match="AUTH_JWT_PRIVATE_KEY"):
        create_app(AuthConfig)


def test_startup_rejects_non_rsa_key():
    class AuthConfig(TestConfig):
        AUTHENTICATION_ON = True
        AUTH_JWT_PRIVATE_KEY = encoded_key(ec.generate_private_key(ec.SECP256R1()))

    with pytest.raises(ValueError, match="RSA private key"):
        create_app(AuthConfig)


def test_startup_rejects_weak_rsa_key():
    class AuthConfig(TestConfig):
        AUTHENTICATION_ON = True
        AUTH_JWT_PRIVATE_KEY = encoded_key(
            rsa.generate_private_key(public_exponent=65537, key_size=1024)
        )

    with pytest.raises(ValueError, match="2048"):
        create_app(AuthConfig)


@pytest.mark.parametrize("key_size", [2048, 3072])
def test_startup_loads_signing_key(key_size):
    key = rsa.generate_private_key(public_exponent=65537, key_size=key_size)

    class AuthConfig(TestConfig):
        AUTHENTICATION_ON = True
        AUTH_JWT_PRIVATE_KEY = encoded_key(key)

    app = create_app(AuthConfig)
    from application.blueprints.auth.auth_token import issue_auth_token

    with app.app_context():
        token = issue_auth_token(subject="123", role="staff", username="alice")
    claims = jwt.decode(
        token,
        key.public_key(),
        algorithms=["RS256"],
        audience=app.config["AUTH_JWT_AUDIENCE"],
        issuer=app.config["AUTH_JWT_ISSUER"],
    )
    assert claims["sub"] == "123"


def test_local_bypass_can_start_without_key():
    class LocalConfig(TestConfig):
        AUTHENTICATION_ON = False
        AUTH_JWT_PRIVATE_KEY = ""

    assert create_app(LocalConfig)
