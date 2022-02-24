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

    page.goto(BASE_URL)
    page.click("text=Add a source")
    assert page.url == f"{BASE_URL}/source/add"

    page.click('input[name="endpoint_url"]')
    page.fill(
        'input[name="endpoint_url"]',
        "http://www.nnjpu.org.uk/publications/docdetail.asp?docid=1593",
    )
    page.select_option('select[name="dataset"]', "brownfield-land", force=True)
    page.select_option(
        'select[name="organisation"]', "local-authority-eng:KET", force=True
    )
    page.click("text=Save and continue")
    assert page.url == f"{BASE_URL}/source/add/summary"

    page.click("text=Save changes to source")
    assert page.url == f"{BASE_URL}/source/add/finish"
