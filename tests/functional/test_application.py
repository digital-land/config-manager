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


def test_add_source(server_process, page):

    page.goto("https://data-manager-prototype.herokuapp.com/")
    # Click text=Add a source
    page.click("text=Add a source")
    # assert page.url == "https://data-manager-prototype.herokuapp.com/source/add"
    # Click text=URL
    page.click("text=URL")
    # Click input[name="endpoint"]
    page.click('input[name="endpoint"]')
    # Fill input[name="endpoint"]
    page.fill('input[name="endpoint"]', "http://www.google.com")
    # Click input[role="textbox"]
    page.click('input[role="textbox"]')
    # Fill input[role="textbox"]
    page.fill('input[role="textbox"]', "ad")
    # Click li[role="option"]:has-text("Address")
    page.click('li[role="option"]:has-text("Address")')
