from config.config import get_request_api_endpoint


def test_request_api_endpoint_uses_explicit_override(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.setenv("REQUEST_API_BASE_URL", "http://request-api:8000")

    assert get_request_api_endpoint() == "http://request-api:8000"


def test_request_api_endpoint_removes_override_trailing_slash(monkeypatch):
    monkeypatch.setenv("REQUEST_API_BASE_URL", "http://host.docker.internal:8000/")

    assert get_request_api_endpoint() == "http://host.docker.internal:8000"


def test_request_api_endpoint_defaults_to_localhost(monkeypatch):
    monkeypatch.delenv("REQUEST_API_BASE_URL", raising=False)
    monkeypatch.setenv("ENVIRONMENT", "local")

    assert get_request_api_endpoint() == "http://localhost:8000"
