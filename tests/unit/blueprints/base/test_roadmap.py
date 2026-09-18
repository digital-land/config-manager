from application.factory import create_app


def _client_with_auth_on():
    app = create_app("config.TestConfig")
    app.config["AUTHENTICATION_ON"] = True
    return app.test_client()


def test_roadmap_page_renders(client):
    response = client.get("/roadmap")

    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "<h1 class=\"govuk-heading-xl\">Roadmap</h1>" in html
    assert "Our progress so far" in html
    assert "What we&#39;re planning to do next" in html or "What we're planning to do next" in html


def test_roadmap_page_does_not_require_login():
    client = _client_with_auth_on()

    response = client.get("/roadmap")

    assert response.status_code == 200
    assert "/auth/login" not in response.headers.get("Location", "")


def test_footer_links_to_roadmap(client):
    response = client.get("/roadmap")

    html = response.get_data(as_text=True)
    assert 'class="govuk-footer__link" href="/roadmap"' in html


def test_home_page_has_roadmap_tile(client):
    response = client.get("/")

    html = response.get_data(as_text=True)
    assert 'class="app-tools-list__item" href="/roadmap"' in html
