import json
import re
from datetime import datetime
from io import BytesIO
from unittest.mock import patch

import responses as rsps

from application.blueprints.base.views import ADD_DATA_LOCK, ASSIGN_ENTITIES_LOCK
from application.db.models import RequestMeta, ServiceLock
from application.extensions import db
from config.config import get_request_api_endpoint

ASYNC_BASE = f"{get_request_api_endpoint()}/requests"


def _selected_entity_checkbox(response_data, reference):
    response_text = response_data.decode()
    match = re.search(
        rf'<input\b[^>]*name="selected_entity_references"[^>]*value="{re.escape(reference)}"[^>]*>',
        response_text,
    )
    assert match, f"Could not find selected_entity_references checkbox for {reference}"
    return match.group(0)


CSV_INPUT = (
    "dataset,resource,organisation,reference,status,entities_created,error_code,message\n"
    "tree,resource-a,local-authority:ABC,ref-1,error,12%,LARGE_NUMBER_OF_NEW_ENTITIES,Entity growth is 12%\n"
    "tree,resource-a,local-authority:ABC,ref-2,passed,1,,\n"
    "article-4-direction-area,resource-b,local-authority:XYZ,ref-3,error,0,CURRENT_RESOURCE_EMPTY,Missing reference\n"
    "article-4-direction-area,resource-b,local-authority:XYZ,ref-4,error,0,"
    "CURRENT_RESOURCE_NO_NEW_ENTITIES,No new entities\n"
    "article-4-direction-area,resource-b,local-authority:XYZ,ref-5,error,0,"
    "DUPLICATE_ENTITY_ALL_FIELDS,Duplicate entities\n"
    "article-4-direction-area,resource-b,local-authority:XYZ,ref-6,error,0,"
    "DUPLICATE_REFERENCE_ORGANISATION_IN_NEW_RESOURCE,Duplicate new resource\n"
    "article-4-direction-area,resource-b,local-authority:XYZ,ref-7,error,0,"
    "DUPLICATE_REFERENCE_ORGANISATION,Duplicate existing\n"
    "article-4-direction-area,resource-b,local-authority:XYZ,ref-8,error,0,"
    "MISSING_ORGANISATION,Missing organisation\n"
    "article-4-direction-area,resource-b,local-authority:XYZ,ref-9,error,0,"
    "MISSING_REFERENCE,Missing reference\n"
    "article-4-direction-area,resource-b,local-authority:XYZ,ref-10,error,0,"
    "INVALID_URI_ISSUE,Invalid URI\n"
    "article-4-direction-area,resource-b,local-authority:XYZ,ref-11,error,0,"
    "PREVIOUS_RESOURCE_NOT_FOUND,Previous resource not found\n"
    "article-4-direction-area,resource-b,local-authority:XYZ,ref-12,error,0,"
    "PREVIOUS_RESOURCE_EMPTY,Previous resource is empty\n"
    "tree,resource-c,local-authority:ABC,ref-4,successful,0,,Success\n"
    "tree,resource-d,local-authority:ABC,ref-5,error,12%,LARGE_NUMBER_OF_NEW_ENTITIES,Entity growth is 12%\n"
    "tree,resource-d,local-authority:ABC,ref-6,error,0,CURRENT_RESOURCE_EMPTY,Missing reference\n"
)


def test_flagged_resources_start_page_loads(client):
    response = client.get("/assign-entities")

    assert response.status_code == 200
    assert b"Assign entities" in response.data
    assert b"Upload the CSV output to see grouped resources" in response.data
    assert (
        b"Use CSV upload when you have a simple batch assign output file"
        in response.data
    )
    assert b"Upload CSV file" in response.data
    assert b"Import from CSV" not in response.data
    assert b"autocomplete-container" not in response.data
    assert b"accessible-autocomplete.min.js" not in response.data


def test_flagged_resources_import_page_loads(client):
    response = client.get("/assign-entities/import")

    assert response.status_code == 200
    assert b"Import simple assign CSV" in response.data
    assert b"CSV format example" in response.data
    assert b"CSV data" in response.data
    assert (
        b"Use the CSV file upload option if your CSV is larger than 10MB."
        in response.data
    )


def test_assign_entities_tile_links_to_start_page(client):
    db.session.query(ServiceLock).filter_by(name=ADD_DATA_LOCK).delete()
    db.session.query(ServiceLock).filter_by(name=ASSIGN_ENTITIES_LOCK).delete()
    db.session.commit()

    response = client.get("/")

    assert response.status_code == 200
    assert b"Assign entities" in response.data
    assert b"/assign-entities" in response.data
    assert response.data.count(b"Lock this process") == 2


def test_add_data_lock_does_not_block_assign_entities(client):
    db.session.query(ServiceLock).filter_by(name=ADD_DATA_LOCK).delete()
    db.session.query(ServiceLock).filter_by(name=ASSIGN_ENTITIES_LOCK).delete()
    db.session.add(
        ServiceLock(
            name=ADD_DATA_LOCK,
            locked_by="someone",
            locked_at=datetime.utcnow(),
        )
    )
    db.session.commit()

    try:
        response = client.get("/assign-entities")
    finally:
        db.session.query(ServiceLock).filter_by(name=ADD_DATA_LOCK).delete()
        db.session.query(ServiceLock).filter_by(name=ASSIGN_ENTITIES_LOCK).delete()
        db.session.commit()

    assert response.status_code == 200


def test_assign_entities_uses_assign_entities_process_lock(client):
    db.session.query(ServiceLock).filter_by(name=ADD_DATA_LOCK).delete()
    db.session.query(ServiceLock).filter_by(name=ASSIGN_ENTITIES_LOCK).delete()
    db.session.add(
        ServiceLock(
            name=ASSIGN_ENTITIES_LOCK,
            locked_by="someone",
            locked_at=datetime.utcnow(),
        )
    )
    db.session.commit()

    try:
        response = client.get("/assign-entities")
    finally:
        db.session.query(ServiceLock).filter_by(name=ADD_DATA_LOCK).delete()
        db.session.query(ServiceLock).filter_by(name=ASSIGN_ENTITIES_LOCK).delete()
        db.session.commit()

    assert response.status_code == 302
    assert "assign_entities_blocked_by=someone" in response.headers["Location"]


def test_assign_entities_card_can_unlock_assign_entities_process(client):
    db.session.query(ServiceLock).filter_by(name=ADD_DATA_LOCK).delete()
    db.session.query(ServiceLock).filter_by(name=ASSIGN_ENTITIES_LOCK).delete()
    db.session.add(
        ServiceLock(
            name=ASSIGN_ENTITIES_LOCK,
            locked_by="someone",
            locked_at=datetime.utcnow(),
        )
    )
    db.session.commit()

    try:
        response = client.get("/")
    finally:
        db.session.query(ServiceLock).filter_by(name=ADD_DATA_LOCK).delete()
        db.session.query(ServiceLock).filter_by(name=ASSIGN_ENTITIES_LOCK).delete()
        db.session.commit()

    assert response.status_code == 200
    assert response.data.count(b"Unlock this process") == 1
    assert response.data.count(b"Lock this process") == 1
    assert b"Locked by <strong>someone</strong>" in response.data
    assert b"/process-lock/assign-entities/toggle" in response.data


def test_unknown_process_lock_redirects_home(client):
    response = client.post("/process-lock/unknown/toggle")

    assert response.status_code == 302
    assert response.headers["Location"] == "/index"


def test_process_lock_toggle_allows_authenticated_non_admin(client, app):
    previous_authentication_on = app.config["AUTHENTICATION_ON"]
    app.config["AUTHENTICATION_ON"] = True
    db.session.query(ServiceLock).filter_by(name=ADD_DATA_LOCK).delete()
    db.session.commit()

    try:
        with client.session_transaction() as sess:
            sess["user"] = {"login": "test-user", "is_admin": False}

        response = client.post("/process-lock/add-data/toggle")
        assert response.status_code == 302
        assert response.headers["Location"] == "/index"
        assert db.session.get(ServiceLock, ADD_DATA_LOCK).locked_by == "test-user"
    finally:
        app.config["AUTHENTICATION_ON"] = previous_authentication_on
        db.session.query(ServiceLock).filter_by(name=ADD_DATA_LOCK).delete()
        db.session.commit()


def test_csv_upload_groups_resource_dataset_combinations(client):
    redirect_response = client.post(
        "/assign-entities/import",
        data={"mode": "parse", "csv_data": CSV_INPUT},
    )

    assert redirect_response.status_code == 302
    assert "/assign-entities/resources" in redirect_response.headers["Location"]
    with client.session_transaction() as sess:
        assert sess.get("flagged_resource_cache_key")

    response = client.post(
        "/assign-entities/import",
        data={"mode": "parse", "csv_data": CSV_INPUT},
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert b"Assign Entities - Flagged Resources" in response.data
    assert b"CSV import results" not in response.data
    assert b"resources require review" in response.data
    assert response.data.count(b">resource-a</button>") == 1
    assert b"resource-b" in response.data
    assert response.data.index(b">resource-a</button>") < response.data.index(
        b"resource-b"
    )
    assert response.data.index(b">resource-a</button>") < response.data.index(
        b">resource-d</button>"
    )
    assert response.data.index(b">resource-d</button>") < response.data.index(
        b"resource-b"
    )
    assert b"resource-c" not in response.data
    assert b"Tree" not in response.data
    assert b"Organisation ABC" not in response.data
    assert b"local-authority:ABC" in response.data
    assert b"Dataset" in response.data
    assert b"Organisation" in response.data
    assert b"Resource" in response.data
    assert b"Errors" in response.data
    assert b"No." in response.data
    assert b"govuk-tag--red" in response.data
    assert b"govuk-tag--orange" in response.data
    assert b"govuk-tag--grey" in response.data
    assert b'name="errors" value="LARGE_NUMBER_OF_NEW_ENTITIES"' in response.data
    assert b">EG</strong>" in response.data
    assert b">CRE</strong>" in response.data
    assert b">NNE</strong>" in response.data
    assert b">DEAF</strong>" in response.data
    assert b">DRON</strong>" in response.data
    assert b">DRO</strong>" in response.data
    assert b">MO</strong>" in response.data
    assert b">MR</strong>" in response.data
    assert b">IUI</strong>" in response.data
    assert b">PRE</strong>" in response.data
    assert b">PRNF</strong>" in response.data
    assert b"Entity growth is above threshold" in response.data
    assert b"Resource empty" in response.data
    assert b"No new entities" in response.data
    assert (
        b"Resource contains duplicates with existing entities (all fields)"
        in response.data
    )
    assert (
        b"Resource contains duplicate entities (organisation and reference)"
        in response.data
    )
    assert (
        b"Resource contains duplicates with existing entities "
        b"(Reference and organisation only)" in response.data
    )
    assert b"Resource contain entities with missing organisation value" in response.data
    assert b"Resource contain entities with missing reference values" in response.data
    assert b"Resource has known issues with invalid URIs" in response.data
    assert b"Previous resource is empty" in response.data
    assert b"Previous resource not found" in response.data
    assert b"Error key" in response.data
    error_key_html = response.data[response.data.index(b"Error key") :]
    assert error_key_html.index(b">EG</strong>") < error_key_html.index(
        b">CRE</strong>"
    )
    assert b"background-color: #ffd8b0" in response.data
    assert b"background-color: #f6d7d2" in response.data
    assert b"background-color: #d4edda" not in response.data
    assert b"White" in response.data
    assert b"Orange" in response.data
    assert b"Entity growth is above threshold" in response.data
    assert b"Entity growth is above threshold (needs careful review)" in response.data
    assert b"Red" in response.data
    assert b"Other errors" in response.data
    assert b"Multiple errors" not in response.data
    assert b"No code" not in response.data


def test_csv_import_rejects_error_rows_without_error_code(client):
    csv_input = (
        "dataset,resource,organisation,reference,status,entities_created,error_code,message\n"
        "tree,resource-a,local-authority:ABC,ref-1,error,12%,,Entity growth is 12%\n"
    )

    response = client.post(
        "/assign-entities/import",
        data={"mode": "parse", "csv_data": csv_input},
    )

    assert response.status_code == 200
    assert (
        b"Rows with status &#39;error&#39; must include an error_code" in response.data
    )


def test_csv_import_preserves_na_error_code(client):
    csv_input = (
        "dataset,resource,organisation,reference,status,entities_created,error_code,message\n"
        "tree,resource-a,local-authority:ABC,ref-1,error,12%,NA,Missing thing\n"
    )

    response = client.post(
        "/assign-entities/import",
        data={"mode": "parse", "csv_data": csv_input},
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert b">NA</strong>" in response.data
    assert b"No code" not in response.data


def test_pasted_csv_import_handles_request_entity_too_large(client, app):
    previous_limit = app.config.get("MAX_CONTENT_LENGTH")
    app.config["MAX_CONTENT_LENGTH"] = 10

    try:
        response = client.post(
            "/assign-entities/import",
            data={"mode": "parse", "csv_data": CSV_INPUT},
        )
    finally:
        app.config["MAX_CONTENT_LENGTH"] = previous_limit

    assert response.status_code == 413
    assert b"The pasted CSV is too large. Upload the CSV file instead." in response.data


def test_uploaded_csv_import_handles_request_entity_too_large(client, app):
    previous_limit = app.config.get("MAX_CONTENT_LENGTH")
    app.config["MAX_CONTENT_LENGTH"] = 10

    try:
        response = client.post(
            "/assign-entities/import",
            data={
                "mode": "upload",
                "csv_file": (BytesIO(CSV_INPUT.encode("utf-8")), "flagged.csv"),
            },
            content_type="multipart/form-data",
        )
    finally:
        app.config["MAX_CONTENT_LENGTH"] = previous_limit

    assert response.status_code == 413
    assert b"The uploaded CSV is too large. Upload a file smaller than 10MB." in (
        response.data
    )


def test_uploaded_csv_groups_resource_dataset_combinations(client):
    response = client.post(
        "/assign-entities/import",
        data={
            "mode": "parse",
            "csv_file": (BytesIO(CSV_INPUT.encode("utf-8")), "flagged.csv"),
        },
        content_type="multipart/form-data",
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert b"Select a resource" in response.data
    assert response.data.count(b">resource-a</button>") == 1
    assert b"resource-b" in response.data


@rsps.activate
def test_assign_entities_check_results_does_not_show_retire_endpoints(client):
    rsps.add(
        rsps.GET,
        f"{ASYNC_BASE}/assign-id-1",
        json={
            "status": "COMPLETE",
            "params": {
                "dataset": "tree",
                "organisation": "local-authority:ABC",
                "resource": "resource-a",
            },
            "response": {
                "data": {
                    "source-summary": {
                        "existing_endpoint_for_organisation_dataset": ["endpoint-a"]
                    },
                    "pipeline-summary": {
                        "new-in-resource": 2,
                        "new-entities": [
                            {
                                "organisation": "local-authority:ABC",
                                "reference": "ref-1",
                            },
                            {
                                "organisation": "local-authority:ABC",
                                "reference": "ref-2",
                            },
                        ],
                    },
                }
            },
        },
        status=200,
    )
    rsps.add(
        rsps.GET,
        f"{ASYNC_BASE}/assign-id-1/response-details",
        json=[
            {
                "entry_number": 1,
                "transformed_row": [
                    {"entity": "1", "field": "reference", "value": "ref-1"},
                    {"entity": "1", "field": "name", "value": "Name 1"},
                ],
                "issue_logs": [],
            },
            {
                "entry_number": 2,
                "transformed_row": [
                    {"entity": "2", "field": "reference", "value": "ref-2"},
                    {"entity": "2", "field": "name", "value": "Name 2"},
                ],
                "issue_logs": [],
            },
        ],
        status=200,
    )

    transform_controller = "application.blueprints.datamanager.controllers.transform"

    with patch(
        f"{transform_controller}.get_endpoint_urls_for_hashes",
        return_value={
            "endpoint-a": {
                "endpoint_url": "https://example.com/data.csv",
                "end_date": "",
            }
        },
    ):
        with patch(f"{transform_controller}.get_org_entity", return_value=90):
            with patch(f"{transform_controller}.get_organisation_name"):
                with patch(
                    f"{transform_controller}.get_dataset_name", return_value="Tree"
                ):
                    with patch(
                        f"{transform_controller}.get_entity_count_for_organisation_and_dataset",
                        return_value=1,
                    ):
                        with patch(
                            f"{transform_controller}.get_entities_for_organisation_and_dataset",
                            return_value=[],
                        ):
                            response = client.get(
                                "/assign-entities/check-results/assign-id-1"
                                "?entity_search=Name+2"
                                "&errors=large_number_of_new_entities,"
                                "current_resource_empty"
                            )

    assert response.status_code == 200
    assert b"Assign Entities - Resource Details" in response.data
    assert b"Search entities" in response.data
    assert b'name="entity_search"' in response.data
    assert b'value="Name 2"' in response.data
    entities_panel = response.data[
        response.data.index(b'id="entities-table"') : response.data.index(
            b'id="transformed-table"'
        )
    ]
    assert b"Name 2" in entities_panel
    assert b"Name 1" not in entities_panel
    assert b"Resource hash" in response.data
    assert b"resource-a" in response.data
    assert b"Endpoints" in response.data
    assert b"https://example.com/data.csv" in response.data
    assert b"endpoint-a" in response.data
    assert b"Errors" in response.data
    assert b"Entity growth is above threshold, Resource empty" in response.data
    assert b"Retire endpoints" not in response.data
    assert b"retire_endpoints" not in response.data
    assert b'action="/assign-entities/check-results/assign-id-1"' in response.data
    assert b'form="duplicate-redirect-form"' in response.data
    assert b"entity-select-all" in response.data
    assert b'name="selected_entity_references"' in response.data
    assert b'value="ref-2" form="duplicate-redirect-form" checked' in response.data
    assert b"1 of 1 entity selected for assignment" in response.data


@rsps.activate
def test_assign_entities_check_results_uses_excluded_references_param(client):
    rsps.add(
        rsps.GET,
        f"{ASYNC_BASE}/assign-selected-id",
        json={
            "status": "COMPLETE",
            "params": {
                "dataset": "tree",
                "organisation": "local-authority:ABC",
                "resource": "resource-a",
                "excluded_references": ["ref-1"],
            },
            "response": {
                "data": {
                    "source-summary": {},
                    "pipeline-summary": {
                        "new-in-resource": 2,
                        "new-entities": [
                            {
                                "entity": "2",
                                "organisation": "local-authority:ABC",
                                "reference": "ref-2",
                            }
                        ],
                    },
                }
            },
        },
        status=200,
    )
    rsps.add(
        rsps.GET,
        f"{ASYNC_BASE}/assign-selected-id/response-details",
        json=[
            {
                "entry_number": 1,
                "transformed_row": [
                    {"entity": "1", "field": "reference", "value": "ref-1"},
                    {"entity": "1", "field": "name", "value": "Name 1"},
                ],
                "issue_logs": [],
            },
            {
                "entry_number": 2,
                "transformed_row": [
                    {"entity": "2", "field": "reference", "value": "ref-2"},
                    {"entity": "2", "field": "name", "value": "Name 2"},
                ],
                "issue_logs": [],
            },
            {
                "entry_number": 3,
                "transformed_row": [
                    {"entity": "3", "field": "reference", "value": "existing-ref"},
                    {"entity": "3", "field": "name", "value": "Existing"},
                ],
                "issue_logs": [],
            },
        ],
        status=200,
    )

    transform_controller = "application.blueprints.datamanager.controllers.transform"
    with patch(f"{transform_controller}.get_org_entity", return_value=90):
        with patch(f"{transform_controller}.get_organisation_name"):
            with patch(f"{transform_controller}.get_dataset_name", return_value="Tree"):
                with patch(
                    f"{transform_controller}.get_entity_count_for_organisation_and_dataset",
                    return_value=1,
                ):
                    with patch(
                        f"{transform_controller}.get_entities_for_organisation_and_dataset",
                        return_value=[
                            {
                                "entity": "3",
                                "reference": "existing-ref",
                                "name": "Existing",
                            }
                        ],
                    ):
                        response = client.get(
                            "/assign-entities/check-results/assign-selected-id"
                        )

    assert response.status_code == 200
    ref_1_checkbox = _selected_entity_checkbox(response.data, "ref-1")
    ref_2_checkbox = _selected_entity_checkbox(response.data, "ref-2")
    existing_checkbox = _selected_entity_checkbox(response.data, "existing-ref")
    assert "checked" not in ref_1_checkbox
    assert "disabled" not in ref_1_checkbox
    assert "checked" in ref_2_checkbox
    assert "disabled" in existing_checkbox
    assert b"1 of 2 entities selected for assignment" in response.data


@rsps.activate
def test_assign_entities_check_results_hides_entity_pagination_when_empty(client):
    rsps.add(
        rsps.GET,
        f"{ASYNC_BASE}/assign-empty-id",
        json={
            "status": "COMPLETE",
            "params": {
                "dataset": "tree",
                "organisation": "local-authority:ABC",
            },
            "response": {
                "data": {
                    "source-summary": {},
                    "pipeline-summary": {"new-in-resource": 0},
                }
            },
        },
        status=200,
    )
    rsps.add(
        rsps.GET,
        f"{ASYNC_BASE}/assign-empty-id/response-details",
        json=[],
        status=200,
    )

    transform_controller = "application.blueprints.datamanager.controllers.transform"

    with patch(f"{transform_controller}.get_org_entity", return_value=90):
        with patch(f"{transform_controller}.get_organisation_name"):
            with patch(f"{transform_controller}.get_dataset_name", return_value="Tree"):
                with patch(
                    f"{transform_controller}.get_entity_count_for_organisation_and_dataset",
                    return_value=0,
                ):
                    with patch(
                        f"{transform_controller}.get_entities_for_organisation_and_dataset",
                        return_value=[],
                    ):
                        response = client.get(
                            "/assign-entities/check-results/assign-empty-id"
                        )

    assert response.status_code == 200
    entities_panel = response.data[
        response.data.index(b'id="entities-table"') : response.data.index(
            b'id="transformed-table"'
        )
    ]
    assert b"Showing entities" not in entities_panel
    assert b'aria-label="Pagination"' not in entities_panel


@rsps.activate
def test_assign_entities_check_results_shows_duplicate_candidates(client):
    geometry = "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))"
    rsps.add(
        rsps.GET,
        f"{ASYNC_BASE}/assign-duplicates-id",
        json={
            "status": "COMPLETE",
            "params": {
                "dataset": "conservation-area",
                "organisation": "local-authority:ABC",
                "resource": "resource-a",
            },
            "response": {
                "data": {
                    "source-summary": {},
                    "pipeline-summary": {
                        "new-in-resource": 1,
                        "old-entity": [
                            {
                                "old-entity": "100",
                                "status": "301",
                                "entity": "200",
                            }
                        ],
                        "duplicate-candidates": [
                            {
                                "old_entity": "100",
                                "entity": "200",
                                "dataset": "conservation-area",
                                "old_reference": "old-ref",
                                "new_reference": "new-ref",
                                "match_type": "complete_match",
                                "notes": (
                                    "Redirect duplicate entity selected in "
                                    "Assign Entities"
                                ),
                                "old_name": "Old Tree",
                                "new_name": "New Tree",
                                "old_entry_date": "2020-01-01",
                                "new_entry_date": "2026-01-01",
                                "old_end_date": "",
                                "new_end_date": "",
                                "name_similarity": 62,
                                "evidence": "name similarity 62%",
                            },
                            {
                                "old_entity": "101",
                                "entity": "201",
                                "dataset": "conservation-area",
                                "old_reference": "old-redirect-ref",
                                "new_reference": "new-redirect-ref",
                                "match_type": "complete_match",
                                "old_entity_redirects": [
                                    {
                                        "old-entity": "101",
                                        "entity": "300",
                                        "status": "301",
                                    }
                                ],
                            },
                        ],
                    },
                }
            },
        },
        status=200,
    )
    rsps.add(
        rsps.GET,
        f"{ASYNC_BASE}/assign-duplicates-id/response-details",
        json=[
            {
                "entry_number": 1,
                "transformed_row": [
                    {"entity": "200", "field": "reference", "value": "new-ref"},
                    {"entity": "200", "field": "name", "value": "New Tree"},
                    {"entity": "200", "field": "geometry", "value": geometry},
                ],
                "issue_logs": [],
            }
        ],
        status=200,
    )

    transform_controller = "application.blueprints.datamanager.controllers.transform"

    with patch(f"{transform_controller}.get_org_entity", return_value=90):
        with patch(f"{transform_controller}.get_organisation_name"):
            with patch(f"{transform_controller}.get_dataset_name", return_value="Tree"):
                with patch(
                    f"{transform_controller}.get_dataset_typology",
                    return_value="entity",
                ):
                    with patch(
                        f"{transform_controller}.get_entity_count_for_organisation_and_dataset",
                        return_value=1,
                    ):
                        with patch(
                            f"{transform_controller}.get_entities_for_organisation_and_dataset",
                            return_value=[],
                        ):
                            response = client.get(
                                "/assign-entities/check-results/assign-duplicates-id"
                            )

    assert response.status_code == 200
    assert b"Dedup" in response.data
    assert b"Match type" not in response.data
    assert b"Entry date" in response.data
    assert b"End date" in response.data
    assert b"Current redirects" in response.data
    assert b"old-ref" in response.data
    assert b"new-ref" in response.data
    assert b"300 (301)" in response.data
    assert b"2020-01-01" in response.data
    assert b"2026-01-01" in response.data
    assert (
        b'href="/assign-entities/check-results/assign-duplicates-id?'
        b'entity_search=200#entities-table"'
    ) in response.data
    assert b'type="hidden" name="entity_redirects"' not in response.data
    assert (
        b'id="entity-redirect-1" name="entity_redirects" type="checkbox"'
        in response.data
    )
    assert (
        b'id="entity-redirect-2" name="entity_redirects" type="checkbox"'
        in response.data
    )
    assert (
        b'id="entity-redirect-2" name="entity_redirects" type="checkbox"'
        b" value=" in response.data
    )
    assert (
        b'id="entity-redirect-2" name="entity_redirects" type="checkbox"'
        b" value="
        b" checked disabled" not in response.data
    )
    assert b"old_entity" in response.data
    first_checkbox = re.search(
        rb'<input[^>]*id="entity-redirect-1"[^>]*>', response.data
    ).group(0)
    assert b"checked" in first_checkbox
    assert b"disabled" in first_checkbox
    assert b"entity-redirect-select-all" in response.data
    assert b"1 of 2 entities selected for redirection" in response.data
    assert b"JSON.parse(checkbox.value" not in response.data
    assert b"function entitySelectionReference(checkbox)" in response.data
    assert b"entitySelectAllCheckbox.addEventListener" in response.data


@rsps.activate
def test_assign_entities_check_results_hides_dedup_for_other_datasets(client):
    rsps.add(
        rsps.GET,
        f"{ASYNC_BASE}/assign-tree-id",
        json={
            "status": "COMPLETE",
            "params": {
                "dataset": "tree",
                "organisation": "local-authority:ABC",
                "resource": "resource-a",
            },
            "response": {
                "data": {
                    "source-summary": {},
                    "pipeline-summary": {
                        "new-in-resource": 1,
                        "duplicate-candidates": [
                            {
                                "old_entity": "100",
                                "entity": "200",
                                "dataset": "tree",
                                "form_value": "{}",
                            }
                        ],
                    },
                }
            },
        },
        status=200,
    )
    rsps.add(
        rsps.GET,
        f"{ASYNC_BASE}/assign-tree-id/response-details",
        json=[],
        status=200,
    )

    transform_controller = "application.blueprints.datamanager.controllers.transform"
    with patch(f"{transform_controller}.get_org_entity", return_value=90):
        with patch(f"{transform_controller}.get_organisation_name"):
            with patch(f"{transform_controller}.get_dataset_name", return_value="Tree"):
                with patch(
                    f"{transform_controller}.get_dataset_typology",
                    return_value="entity",
                ):
                    with patch(
                        f"{transform_controller}.get_entity_count_for_organisation_and_dataset",
                        return_value=1,
                    ):
                        with patch(
                            f"{transform_controller}.get_entities_for_organisation_and_dataset",
                            return_value=[],
                        ):
                            response = client.get(
                                "/assign-entities/check-results/assign-tree-id"
                            )

    assert response.status_code == 200
    assert b"Dedup" not in response.data
    assert b'href="#duplicates-table"' not in response.data
    assert b'id="duplicates-table"' not in response.data


def test_assign_entities_check_results_post_stores_redirects(client):
    request_id = "assign-post-id"
    with patch(
        "application.blueprints.datamanager.router.fetch_request",
        return_value={
            "response": {
                "data": {
                    "pipeline-summary": {
                        "duplicate-candidates": [
                            {
                                "old_entity": "100",
                                "entity": "200",
                                "dataset": "tree",
                            }
                        ]
                    }
                }
            }
        },
    ):
        response = client.post(
            f"/assign-entities/check-results/{request_id}",
            data={
                "entity_redirects": [
                    json.dumps(
                        {
                            "old_entity": "100",
                            "entity": "200",
                            "dataset": "tree",
                            "old_reference": "old-ref",
                            "new_reference": "new-ref",
                            "match_type": "complete_match",
                            "notes": (
                                "Redirect duplicate entity selected in Assign Entities"
                            ),
                        }
                    ),
                    json.dumps(
                        {
                            "old_entity": "999",
                            "entity": "200",
                            "dataset": "tree",
                        }
                    ),
                ]
            },
        )

    assert response.status_code == 302
    assert response.headers["Location"].endswith(
        f"/datamanager/add-data/{request_id}/entities"
    )
    meta = db.session.get(RequestMeta, request_id)
    assert json.loads(meta.entity_redirects) == [
        {
            "old_entity": "100",
            "entity": "200",
            "dataset": "tree",
            "old_reference": "old-ref",
            "new_reference": "new-ref",
            "match_type": "complete_match",
            "notes": "Redirect duplicate entity selected in Assign Entities",
        }
    ]


def test_assign_entities_check_results_post_resubmits_changed_entity_selection(client):
    request_id = "assign-selection-id"
    selected_value = "ref-2"
    selected_redirect = json.dumps(
        {
            "old_entity": "100",
            "entity": "200",
            "dataset": "tree",
            "old_reference": "old-ref",
            "new_reference": "ref-2",
            "match_type": "complete_match",
        }
    )
    excluded_redirect = json.dumps(
        {
            "old_entity": "101",
            "entity": "201",
            "dataset": "tree",
            "old_reference": "old-ref-1",
            "new_reference": "ref-1",
            "match_type": "complete_match",
        }
    )
    with patch(
        "application.blueprints.datamanager.router.fetch_request",
        return_value={
            "params": {
                "dataset": "tree",
                "resource": "resource-a",
                "organisation": "local-authority:ABC",
                "return_endpoint": "assign_entities.flagged_resources_summary",
            },
            "response": {
                "data": {
                    "pipeline-summary": {
                        "new-entities": [
                            {
                                "organisation": "local-authority:ABC",
                                "reference": "ref-2",
                            },
                        ],
                        "duplicate-candidates": [
                            {
                                "old_entity": "100",
                                "entity": "200",
                                "dataset": "tree",
                            },
                            {
                                "old_entity": "101",
                                "entity": "201",
                                "dataset": "tree",
                            },
                        ],
                    }
                }
            },
        },
    ):
        with patch(
            "application.blueprints.datamanager.router._submit_assign_entities_request",
            return_value="replacement-id",
        ) as submit_request:
            response = client.post(
                f"/assign-entities/check-results/{request_id}",
                data={
                    "entity_selection_changed": "true",
                    "visible_entity_references": [
                        "ref-1",
                        selected_value,
                    ],
                    "selected_entity_references": [selected_value],
                    "entity_redirects": [
                        selected_redirect,
                        excluded_redirect,
                        json.dumps(
                            {
                                "old_entity": "999",
                                "entity": "200",
                                "dataset": "tree",
                                "new_reference": "ref-2",
                            }
                        ),
                    ],
                },
            )

    assert response.status_code == 302
    assert response.headers["Location"].endswith(
        "/assign-entities/check-results/replacement-id"
    )
    submit_request.assert_called_once_with(
        "tree",
        "resource-a",
        organisation="local-authority:ABC",
        return_endpoint="assign_entities.flagged_resources_summary",
        excluded_references=["ref-1"],
        selected_redirects=[
            {
                "reference": "ref-2",
                "old_entity_number": "100",
            }
        ],
    )
    assert db.session.get(RequestMeta, request_id) is None


def test_assign_entities_check_results_post_continues_for_unchanged_entity_selection(
    client,
):
    request_id = "assign-unchanged-id"
    selected_values = ["ref-1", "ref-2"]
    with patch(
        "application.blueprints.datamanager.router.fetch_request",
        return_value={
            "params": {
                "dataset": "tree",
                "resource": "resource-a",
                "organisation": "local-authority:ABC",
            },
            "response": {
                "data": {
                    "pipeline-summary": {
                        "new-entities": [
                            {
                                "organisation": "local-authority:ABC",
                                "reference": "ref-1",
                            },
                            {
                                "organisation": "local-authority:ABC",
                                "reference": "ref-2",
                            },
                        ],
                        "duplicate-candidates": [],
                    }
                }
            },
        },
    ):
        with patch(
            "application.blueprints.datamanager.router._submit_assign_entities_request"
        ) as submit_request:
            response = client.post(
                f"/assign-entities/check-results/{request_id}",
                data={"selected_entity_references": selected_values},
            )

    assert response.status_code == 302
    assert response.headers["Location"].endswith(
        f"/datamanager/add-data/{request_id}/entities"
    )
    submit_request.assert_not_called()
    meta = db.session.get(RequestMeta, request_id)
    assert json.loads(meta.entity_redirects) == []


def test_assign_entities_check_results_post_resubmits_changed_redirect_selection(
    client,
):
    request_id = "assign-redirect-selection-id"
    selected_values = ["ref-1", "ref-2"]
    selected_redirect = json.dumps(
        {
            "old_entity": "100",
            "entity": "200",
            "dataset": "tree",
            "new_reference": "ref-1",
        }
    )
    with patch(
        "application.blueprints.datamanager.router.fetch_request",
        return_value={
            "params": {
                "dataset": "tree",
                "resource": "resource-a",
                "organisation": "local-authority:ABC",
            },
            "response": {
                "data": {
                    "pipeline-summary": {
                        "new-entities": [
                            {
                                "organisation": "local-authority:ABC",
                                "reference": "ref-1",
                            },
                            {
                                "organisation": "local-authority:ABC",
                                "reference": "ref-2",
                            },
                        ],
                        "duplicate-candidates": [
                            {
                                "old_entity": "100",
                                "entity": "200",
                                "dataset": "tree",
                            }
                        ],
                    }
                }
            },
        },
    ):
        with patch(
            "application.blueprints.datamanager.router._submit_assign_entities_request",
            return_value="replacement-id",
        ) as submit_request:
            response = client.post(
                f"/assign-entities/check-results/{request_id}",
                data={
                    "entity_selection_changed": "true",
                    "visible_entity_references": selected_values,
                    "selected_entity_references": selected_values,
                    "entity_redirects": [selected_redirect],
                },
            )

    assert response.status_code == 302
    assert response.headers["Location"].endswith(
        "/assign-entities/check-results/replacement-id"
    )
    submit_request.assert_called_once_with(
        "tree",
        "resource-a",
        organisation="local-authority:ABC",
        return_endpoint="assign_entities.flagged_resources_start",
        excluded_references=[],
        selected_redirects=[
            {
                "reference": "ref-1",
                "old_entity_number": "100",
            }
        ],
    )


@rsps.activate
def test_resource_link_submits_assign_entities_request(client):
    import_response = client.post(
        "/assign-entities/import",
        data={"csv_data": CSV_INPUT},
    )
    assert import_response.status_code == 302
    rsps.add(rsps.POST, ASYNC_BASE, json={"id": "assign-id-1"}, status=202)

    with patch(
        "application.blueprints.datamanager.controllers.flagged_resources.get_dataset_id",
        return_value=None,
    ):
        with patch(
            "application.blueprints.datamanager.controllers.flagged_resources.get_dataset_name",
            return_value="Tree",
        ):
            with patch(
                "application.blueprints.datamanager.controllers.flagged_resources.get_collection_id",
                return_value="tree",
            ):
                with patch(
                    "application.blueprints.datamanager.controllers.flagged_resources.get_resource",
                    return_value=[
                        {
                            "pipeline": "tree",
                            "organisation": "local-authority:ABC",
                        }
                    ],
                ):
                    response = client.post(
                        "/assign-entities/resource",
                        data={
                            "dataset": "tree",
                            "resource": "resource-a",
                            "organisation": "local-authority:ABC",
                            "errors": "large_number_of_new_entities,current_resource_empty",
                        },
                    )

    assert response.status_code == 302
    location = response.headers["Location"]
    assert "/assign-entities/check-results/assign-id-1" in location
    assert "errors=large_number_of_new_entities,current_resource_empty" in location
    assert len(rsps.calls) == 1
    assert rsps.calls[0].request.url == ASYNC_BASE
    assert rsps.calls[0].request.headers["Content-Type"] == "application/json"
    assert json.loads(rsps.calls[0].request.body) == {
        "params": {
            "type": "add_data",
            "resource": "resource-a",
            "dataset": "tree",
            "collection": "tree",
            "authoritative": True,
            "github_branch": "config-manager-update",
            "organisationName": "local-authority:ABC",
            "organisation": "local-authority:ABC",
            "return_endpoint": "assign_entities.flagged_resources_summary",
        }
    }


@rsps.activate
def test_direct_dataset_resource_skips_summary_page(client):
    rsps.add(rsps.POST, ASYNC_BASE, json={"id": "assign-id-1"}, status=202)

    with patch(
        "application.blueprints.datamanager.controllers.flagged_resources.get_dataset_id",
        return_value="tree",
    ):
        with patch(
            "application.blueprints.datamanager.controllers.flagged_resources.get_dataset_name",
            return_value="Tree",
        ):
            with patch(
                "application.blueprints.datamanager.controllers.flagged_resources.get_collection_id",
                return_value="tree",
            ):
                with patch(
                    "application.blueprints.datamanager.controllers.flagged_resources.get_resource",
                    return_value=[
                        {
                            "pipeline": "tree",
                            "organisation": "local-authority:ABC",
                        }
                    ],
                ):
                    response = client.post(
                        "/assign-entities",
                        data={"dataset": "Tree", "resource": "resource-a"},
                    )

    assert response.status_code == 302
    assert "/assign-entities/check-results/assign-id-1" in response.headers["Location"]
    params = json.loads(rsps.calls[0].request.body)["params"]
    assert params["github_branch"] == "config-manager-update"
    assert params["return_endpoint"] == "assign_entities.flagged_resources_start"


@rsps.activate
def test_resource_submit_uses_selected_organisation(client):
    rsps.add(rsps.POST, ASYNC_BASE, json={"id": "assign-id-1"}, status=202)

    with patch(
        "application.blueprints.datamanager.controllers.flagged_resources.get_dataset_id",
        return_value=None,
    ):
        with patch(
            "application.blueprints.datamanager.controllers.flagged_resources.get_dataset_name",
            return_value="Tree",
        ):
            with patch(
                "application.blueprints.datamanager.controllers.flagged_resources.get_collection_id",
                return_value="tree",
            ):
                with patch(
                    "application.blueprints.datamanager.controllers.flagged_resources.get_resource"
                ) as get_resource:
                    response = client.post(
                        "/assign-entities/resource",
                        data={
                            "dataset": "tree",
                            "resource": "resource-a",
                            "organisation": "local-authority:XYZ",
                        },
                    )

    assert response.status_code == 302
    get_resource.assert_not_called()
    assert json.loads(rsps.calls[0].request.body)["params"]["github_branch"] == (
        "config-manager-update"
    )
    assert json.loads(rsps.calls[0].request.body)["params"]["organisation"] == (
        "local-authority:XYZ"
    )
    assert json.loads(rsps.calls[0].request.body)["params"]["return_endpoint"] == (
        "assign_entities.flagged_resources_summary"
    )
