from tests.functional.conftest import BASE_URL


def test_visit_main_pages(server_process, page):

    page.goto(BASE_URL)
    assert page.inner_text("h1") == "Data manager"

    page.click("text=Add a source")
    assert page.url == f"{BASE_URL}/source/add"

    page.click("text=Data manager")
    page.click("text=Edit source")
    assert page.url == f"{BASE_URL}/source/"

    page.click("text=Data manager")
    page.click("text=Create resource mappings")
    assert page.url == f"{BASE_URL}/#"

    page.click("text=Data manager")
