import json
from datetime import datetime
from io import BytesIO
from unittest.mock import patch

import responses as rsps

from application.db.models import ServiceLock
from application.extensions import db
from config.config import get_request_api_endpoint

ASYNC_BASE = f"{get_request_api_endpoint()}/requests"


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
    response = client.get("/asign-entities")

    assert response.status_code == 200
    assert b"Assign entities" in response.data
    assert b"Review resources flagged by the simple assign process" in response.data
    assert b"Use CSV import when you have a simple assign output file" in response.data
    assert b"autocomplete-container" in response.data
    assert b"accessible-autocomplete.min.js" in response.data
    assert b"Import from CSV" in response.data
    assert b"Upload CSV file" in response.data


def test_flagged_resources_import_page_loads(client):
    response = client.get("/asign-entities/import")

    assert response.status_code == 200
    assert b"Import simple assign CSV" in response.data
    assert b"CSV format example" in response.data
    assert b"CSV data" in response.data


def test_assign_entities_tile_links_to_start_page(client):
    db.session.query(ServiceLock).filter_by(name="add_data").delete()
    db.session.commit()

    response = client.get("/")

    assert response.status_code == 200
    assert b"Assign entities" in response.data
    assert b"/asign-entities" in response.data
    assert response.data.count(b"Lock this process") == 2


def test_assign_entities_uses_add_data_process_lock(client):
    db.session.query(ServiceLock).filter_by(name="add_data").delete()
    db.session.add(
        ServiceLock(
            name="add_data",
            locked_by="someone",
            locked_at=datetime.utcnow(),
        )
    )
    db.session.commit()

    try:
        response = client.get("/asign-entities")
    finally:
        db.session.query(ServiceLock).filter_by(name="add_data").delete()
        db.session.commit()

    assert response.status_code == 302
    assert "add_data_blocked_by=someone" in response.headers["Location"]


def test_assign_entities_card_can_unlock_shared_process(client):
    db.session.query(ServiceLock).filter_by(name="add_data").delete()
    db.session.add(
        ServiceLock(
            name="add_data",
            locked_by="someone",
            locked_at=datetime.utcnow(),
        )
    )
    db.session.commit()

    try:
        response = client.get("/")
    finally:
        db.session.query(ServiceLock).filter_by(name="add_data").delete()
        db.session.commit()

    assert response.status_code == 200
    assert response.data.count(b"Unlock this process") == 2
    assert b"Locked by <strong>someone</strong>" in response.data


def test_csv_upload_groups_resource_dataset_combinations(client):
    redirect_response = client.post(
        "/asign-entities/import",
        data={"mode": "parse", "csv_data": CSV_INPUT},
    )

    assert redirect_response.status_code == 302
    assert "/asign-entities/resources" in redirect_response.headers["Location"]
    with client.session_transaction() as sess:
        assert sess.get("flagged_resource_cache_key")

    response = client.post(
        "/asign-entities/import",
        data={"mode": "parse", "csv_data": CSV_INPUT},
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert b"Flagged resources" in response.data
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
    assert b"LARGE_NUMBER_OF_NEW_ENTITIES" not in response.data
    assert b"CURRENT_RESOURCE_EMPTY" not in response.data
    assert b"CURRENT_RESOURCE_NO_NEW_ENTITIES" not in response.data
    assert b"DUPLICATE_ENTITY_ALL_FIELDS" not in response.data
    assert b"DUPLICATE_REFERENCE_ORGANISATION_IN_NEW_RESOURCE" not in response.data
    assert b"DUPLICATE_REFERENCE_ORGANISATION" not in response.data
    assert b"MISSING_ORGANISATION" not in response.data
    assert b"MISSING_REFERENCE" not in response.data
    assert b"INVALID_URI_ISSUE" not in response.data
    assert b"PREVIOUS_RESOURCE_EMPTY" not in response.data
    assert b"PREVIOUS_RESOURCE_NOT_FOUND" not in response.data
    assert b"No code" not in response.data


def test_csv_import_rejects_error_rows_without_error_code(client):
    csv_input = (
        "dataset,resource,organisation,reference,status,entities_created,error_code,message\n"
        "tree,resource-a,local-authority:ABC,ref-1,error,12%,,Entity growth is 12%\n"
    )

    response = client.post(
        "/asign-entities/import",
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
        "/asign-entities/import",
        data={"mode": "parse", "csv_data": csv_input},
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert b">NA</strong>" in response.data
    assert b"No code" not in response.data


def test_uploaded_csv_groups_resource_dataset_combinations(client):
    response = client.post(
        "/asign-entities/import",
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


def test_flagged_resource_detail_post_redirects_to_preview(client):
    response = client.post(
        "/asign-entities/check-results/assign-id-1",
        data={"retire_endpoints": ["endpoint-a"]},
    )

    assert response.status_code == 302
    assert "/datamanager/add-data/assign-id-1/entities" in response.headers["Location"]


@rsps.activate
def test_resource_link_submits_assign_entities_request(client):
    import_response = client.post(
        "/asign-entities/import",
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
                        "/asign-entities/resource",
                        data={
                            "dataset": "tree",
                            "resource": "resource-a",
                            "organisation": "local-authority:ABC",
                        },
                    )

    assert response.status_code == 302
    assert "/asign-entities/check-results/assign-id-1" in response.headers["Location"]
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
            "organisationName": "local-authority:ABC",
            "organisation": "local-authority:ABC",
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
                        "/asign-entities",
                        data={"dataset": "Tree", "resource": "resource-a"},
                    )

    assert response.status_code == 302
    assert "/asign-entities/check-results/assign-id-1" in response.headers["Location"]


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
                        "/asign-entities/resource",
                        data={
                            "dataset": "tree",
                            "resource": "resource-a",
                            "organisation": "local-authority:XYZ",
                        },
                    )

    assert response.status_code == 302
    get_resource.assert_not_called()
    assert json.loads(rsps.calls[0].request.body)["params"]["organisation"] == (
        "local-authority:XYZ"
    )
