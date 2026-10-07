from unittest.mock import patch

import pytest

from application.blueprints.datamanager.controllers.preview import (
    _build_entity_organisation_summary,
)

FORM = "application.blueprints.datamanager.controllers.form"
SOURCE = "government-organisation:MHCLG"
OWNER = "local-authority:MAN"
ORGS = [{"code": SOURCE, "label": "MHCLG"}, {"code": OWNER, "label": "Manchester"}]
CHECK = {
    "params": {
        "dataset": "conservation-area",
        "collection": "conservation-area",
        "organisationName": SOURCE,
        "url": "https://example.org/data.csv",
    }
}


@pytest.fixture
def form_services(client):
    with client.session_transaction() as sess:
        sess.pop("add_data_fields", None)
    with patch(f"{FORM}.get_dataset_id", return_value="conservation-area"), patch(
        f"{FORM}.get_collection_id", return_value="conservation-area"
    ), patch(f"{FORM}._get_org_values_for_dataset", return_value=ORGS), patch(
        f"{FORM}.is_valid_organisation", return_value=True
    ), patch(
        f"{FORM}.fetch_request", return_value=CHECK
    ), patch(
        f"{FORM}.submit_request", return_value="new-request"
    ) as submit, patch(
        f"{FORM}.record_branch_baseline"
    ), patch(
        f"{FORM}.record_source_flow"
    ):
        yield submit


def form_data(authoritative="no", owner=OWNER):
    return {
        "dataset": "Conservation area",
        "organisation": SOURCE,
        "endpoint_url": "https://example.org/data.csv",
        "documentation_url": "https://example.org/docs",
        "licence": "ogl3",
        "start_day": "1",
        "start_month": "1",
        "start_year": "2026",
        "authoritative": authoritative,
        "authoritative_organisation": owner,
        "github_new": "true",
    }


@pytest.mark.parametrize(
    "authoritative,owner,expected",
    [
        ("no", OWNER, OWNER),
        ("no", "", None),
        ("yes", OWNER, None),
        ("yes", SOURCE, None),
    ],
)
@pytest.mark.parametrize("initial", [True, False])
def test_owner_reaches_preview_without_changing_source(
    client, form_services, authoritative, owner, expected, initial
):
    url = "/datamanager/" if initial else "/datamanager/add-data/check-id"
    response = client.post(url, data=form_data(authoritative, owner))
    assert response.status_code == 302
    if initial:
        # The check uses the source; the owner is bound to the returned check ID.
        assert form_services.call_args.args[0]["organisationName"] == SOURCE
        response = client.get("/datamanager/add-data/new-request")
        assert response.status_code == 302
    params = form_services.call_args.args[0]
    assert params["authoritative_organisation"] == expected
    assert params["authoritative"] is (authoritative == "yes")
    assert params["organisation"] == params["organisationName"] == SOURCE


def test_other_check_does_not_reuse_owner(client, form_services):
    client.post("/datamanager/", data=form_data())
    form_services.reset_mock()
    response = client.get("/datamanager/add-data/other-check")
    assert response.status_code == 200
    assert f'value="{OWNER}" selected'.encode() not in response.data
    form_services.assert_not_called()


@pytest.mark.parametrize("invalid", ["dataset", "source"])
def test_preview_revalidates_owner_against_check(client, form_services, invalid):
    client.post("/datamanager/", data=form_data())
    form_services.reset_mock()
    params = {**CHECK["params"]}
    if invalid == "source":
        params["organisationName"] = OWNER
    else:
        params["dataset"] = "another-dataset"
    with patch(f"{FORM}.fetch_request", return_value={"params": params}), patch(
        f"{FORM}._get_org_values_for_dataset",
        side_effect=lambda dataset: ORGS if dataset == "conservation-area" else [],
    ):
        response = client.get("/datamanager/add-data/new-request")
    assert response.status_code == 200
    assert b"Select an authoritative organisation" in response.data
    form_services.assert_not_called()
    # A corrected POST must be processed even when session fields are complete.
    response = client.post(
        "/datamanager/add-data/new-request", data=form_data(owner="")
    )
    assert response.status_code == 302
    assert form_services.call_args.args[0]["authoritative_organisation"] is None


@pytest.mark.parametrize("original_id", ["new-request", "unrelated-check"])
def test_recheck_only_transfers_its_own_form_data(client, form_services, original_id):
    client.post("/datamanager/", data=form_data())
    check_controller = "application.blueprints.datamanager.controllers.check"
    with patch(f"{check_controller}.fetch_request", return_value=CHECK), patch(
        f"{check_controller}.submit_request", return_value="rechecked-id"
    ):
        response = client.post(f"/datamanager/check-results/{original_id}")
    assert response.status_code == 302
    form_services.reset_mock()
    response = client.get("/datamanager/add-data/rechecked-id")
    if original_id == "new-request":
        assert response.status_code == 302
        assert form_services.call_args.args[0]["authoritative_organisation"] == OWNER
    else:
        assert response.status_code == 200
        form_services.assert_not_called()


@pytest.mark.parametrize("url", ["/datamanager/", "/datamanager/add-data/check-id"])
def test_unprovisioned_owner_rejected(client, form_services, url):
    response = client.post(url, data=form_data(owner="local-authority:OTHER"))
    assert response.status_code == 200
    assert (
        b"Select an authoritative organisation that can provide this dataset"
        in response.data
    )
    form_services.assert_not_called()


@pytest.mark.parametrize("url", ["/datamanager/", "/datamanager/add-data/check-id"])
def test_source_cannot_be_selected_as_owner(client, form_services, url):
    data = form_data(owner=SOURCE)
    if url != "/datamanager/":
        # The fallback form must use the source from the check, not a posted value.
        data["organisation"] = OWNER
    response = client.post(url, data=data)
    assert response.status_code == 200
    assert (
        response.data.count(
            b"Select an authoritative organisation different from the source organisation"
        )
        == 2
    )
    assert f'value="{SOURCE}" selected'.encode() in response.data
    form_services.assert_not_called()


def test_owner_retained_on_validation_error(client, form_services):
    data = form_data()
    data["endpoint_url"] = ""
    response = client.post("/datamanager/", data=data)
    assert response.status_code == 200
    assert f'value="{OWNER}" selected'.encode() in response.data
    assert b"Source organisation" in response.data
    form_services.assert_not_called()


@pytest.mark.parametrize(
    "overlap,error", [(False, False), (True, False), (False, True)]
)
def test_non_authoritative_owner_preview_preserves_range_safeguards(overlap, error):
    entry = {
        "dataset": "conservation-area",
        "organisation": OWNER,
        "overlap": overlap,
        "error": error,
    }
    if not overlap and not error:
        entry.update({"entity-minimum": 100, "entity-maximum": 101})
    table, shown, warning, overlap_info, error_warning = (
        _build_entity_organisation_summary(
            [{"entity": 100}], False, {"entity-organisation": [entry]}, OWNER
        )
    )
    assert shown is (not overlap and not error)
    assert warning is None
    assert bool(overlap_info) is overlap
    assert bool(error_warning) is error
    if shown:
        assert OWNER in str(table)
