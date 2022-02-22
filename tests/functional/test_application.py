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

    page.click('input[name="endpoint"]')
    page.fill(
        'input[name="endpoint"]',
        "http://www.nnjpu.org.uk/publications/docdetail.asp?docid=1593",
    )
    page.select_option('select[name="dataset"]', "brownfield-land")
    page.select_option('select[name="organisation"]', "local-authority-eng:KET")
    page.click("text=Save and continue")

    assert page.url == f"{BASE_URL}/source/add/summary"
    page.click("text=Url")

    to_check = [
        ("Url", "http://www.nnjpu.org.uk/publications/docdetail.asp?docid=1593"),
        ("Dataset", "brownfield-land"),
        ("Organisation", "local-authority-eng:KET"),
    ]

    for index, items in enumerate(to_check):

        dt_text = page.text_content(
            f"dl > .govuk-summary-list__row:nth-child({index+1}) > dt"
        ).strip()
        dd_text = page.text_content(
            f"dl > .govuk-summary-list__row:nth-child({index+1}) > dd"
        ).strip()

        assert dt_text == items[0]
        assert dd_text == items[1]

    page.click("text=Save source")
    assert page.url == f"{BASE_URL}/source/add/finish"
