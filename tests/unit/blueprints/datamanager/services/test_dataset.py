from application.blueprints.datamanager.services.dataset import _url_for_logging


def test_url_for_logging_removes_credentials_query_and_fragment():
    assert (
        _url_for_logging(
            "https://user:password@example.com:8443/path?token=secret#fragment"
        )
        == "https://example.com:8443/path"
    )
