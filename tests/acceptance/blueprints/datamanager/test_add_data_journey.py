"""
Acceptance test: full add-data journey for article-4-direction-area.

Flow:
  1. Import from CSV (paste endpoint config)
  2. Submit the pre-filled form (authoritative=yes)
  3. View check results — 'geom' column shows as unmapped
  4. Apply column mapping (geom → geometry) and recheck
  5. Navigate to Check Entities (add_data preview)
  6. Entities preview: 3 new entities, endpoint not in system
"""

from unittest.mock import patch

import responses as rsps

ASYNC_BASE = "http://localhost:8000/requests"

CSV_INPUT = (
    "organisation,pipelines,documentation-url,endpoint-url,start-date,plugin,licence\n"
    "local-authority:SKP,article-4-direction-area,"
    "https://www.stockport.gov.uk/planning-development-open-datasets,"
    "https://raw.githubusercontent.com/digital-land/PublishExamples/refs/heads/main/"
    "Article4Direction/Files/Article4DirectionArea/articel4directionareas-(wrongColName-NewRefs).csv,"
    "2026-01-22,,ogl3"
)

ENDPOINT_URL = (
    "https://raw.githubusercontent.com/digital-land/PublishExamples/refs/heads/main/"
    "Article4Direction/Files/Article4DirectionArea/articel4directionareas-(wrongColName-NewRefs).csv"
)

# --- Async API mock payloads ---

CHECK_RESULT_COMPLETE = {
    "id": "check-id-1",
    "status": "COMPLETE",
    "modified": "2026-02-24T12:00:00Z",
    "response": {
        "data": {
            "column-field-log": [
                {"column": "reference", "field": "reference", "missing": False},
                {"column": "name", "field": "name", "missing": False},
                {"column": "start-date", "field": "start-date", "missing": False},
            ],
            "error-summary": [],
        }
    },
    "params": {
        "organisationName": "local-authority:SKP",
        "dataset": "article-4-direction-area",
        "url": ENDPOINT_URL,
        "collection": "article-4-direction",
    },
}

# Response details include a 'geom' column not in column-field-log → shows as unmapped
RESP_DETAILS_WITH_GEOM = [
    {
        "entry_number": 1,
        "converted_row": {
            "reference": "A4a-14-test",
            "name": "Test Direction Area 14",
            "start-date": "2026-01-22",
            "geom": "MULTIPOLYGON(((-2.1 53.4,-2.0 53.4,-2.0 53.5,-2.1 53.5,-2.1 53.4)))",
        },
        "transformed_row": [],
        "issue_logs": [],
    }
]

RECHECK_RESULT_COMPLETE = {
    "id": "check-id-2",
    "status": "COMPLETE",
    "modified": "2026-02-24T12:01:00Z",
    "response": {
        "data": {
            "column-field-log": [
                {"column": "reference", "field": "reference", "missing": False},
                {"column": "name", "field": "name", "missing": False},
                {"column": "start-date", "field": "start-date", "missing": False},
                {"column": "geom", "field": "geometry", "missing": False},
            ],
            "error-summary": [],
        }
    },
    "params": {
        "organisationName": "local-authority:SKP",
        "dataset": "article-4-direction-area",
        "url": ENDPOINT_URL,
        "collection": "article-4-direction",
        "column_mapping": {"geom": "geometry"},
    },
}

ENTITIES_PREVIEW_COMPLETE = {
    "id": "preview-id-1",
    "status": "COMPLETE",
    "response": {
        "data": {
            "pipeline-summary": {
                "new-in-resource": 3,
                "existing-in-resource": 0,
                "new-entities": [
                    {
                        "reference": "A4a-14-test",
                        "prefix": "article-4-direction-area",
                        "organisation": "local-authority:SKP",
                        "entity": "7010009611",
                    },
                    {
                        "reference": "A4a-15-test",
                        "prefix": "article-4-direction-area",
                        "organisation": "local-authority:SKP",
                        "entity": "7010009612",
                    },
                    {
                        "reference": "A4a-16-test",
                        "prefix": "article-4-direction-area",
                        "organisation": "local-authority:SKP",
                        "entity": "7010009613",
                    },
                ],
                "existing-entities": [],
                "pipeline-issues": [],
                "entity-organisation": [],
            },
            "endpoint-summary": {
                "endpoint_url_in_endpoint_csv": False,
                "new_endpoint_entry": {
                    "endpoint": "abc123endpoint",
                    "endpoint-url": ENDPOINT_URL,
                    "parameters": "",
                    "plugin": "",
                    "entry-date": "2026-01-22",
                    "start-date": "2026-01-22",
                    "end-date": "",
                },
            },
            "source-summary": {
                "documentation_url_in_source_csv": False,
                "new_source_entry": {
                    "source": "src123",
                    "attribution": "",
                    "collection": "article-4-direction",
                    "documentation-url": "https://www.stockport.gov.uk/planning-development-open-datasets",
                    "endpoint": "abc123endpoint",
                    "licence": "ogl3",
                    "organisation": "local-authority:SKP",
                    "pipelines": "article-4-direction-area",
                    "entry-date": "2026-01-22",
                    "start-date": "2026-01-22",
                    "end-date": "",
                },
            },
        }
    },
    "params": {
        "dataset": "article-4-direction-area",
        "authoritative": True,
        "column_mapping": {"geom": "geometry"},
    },
}


class TestAddDataJourney:
    # --- Step 1: Import from CSV ---

    def test_import_page_loads(self, client):
        response = client.get("/datamanager/import")
        assert response.status_code == 200
        assert b"Import" in response.data

    def test_import_csv_paste_redirects_to_prefilled_form(self, client):
        response = client.post(
            "/datamanager/import",
            data={"mode": "parse", "csv_data": CSV_INPUT},
        )
        assert response.status_code == 302
        location = response.headers["Location"]
        assert "import_data=true" in location
        assert "article-4-direction-area" in location
        assert "local-authority" in location

    # --- Step 2: Submit the pre-filled form ---

    @rsps.activate
    def test_form_submission_redirects_to_check_results(self, client):
        rsps.add(rsps.POST, ASYNC_BASE, json={"id": "check-id-1"}, status=202)
        with patch("application.blueprints.datamanager.controllers.form.get_dataset_id", return_value="article-4-direction-area"):
            with patch("application.blueprints.datamanager.controllers.form.get_collection_id", return_value="article-4-direction"):
                with patch("application.blueprints.datamanager.controllers.form.get_provision_orgs_for_dataset", return_value=["local-authority:SKP"]):
                    with patch("application.blueprints.datamanager.controllers.form.is_valid_organisation", return_value=True):
                        with patch("application.blueprints.datamanager.controllers.form.format_org_options", return_value=[{"code": "local-authority:SKP", "label": "Stockport MBC"}]):
                            response = client.post("/datamanager/", data={
                                "dataset": "article-4-direction-area",
                                "organisation": "local-authority:SKP",
                                "endpoint_url": ENDPOINT_URL,
                                "documentation_url": "https://www.stockport.gov.uk/planning-development-open-datasets",
                                "licence": "ogl3",
                                "start_day": "22",
                                "start_month": "1",
                                "start_year": "2026",
                                "authoritative": "yes",
                            })
        assert response.status_code == 302
        assert "check-results/check-id-1" in response.headers["Location"]

    # --- Step 3: Check results — 'geom' appears as unmapped ---

    @rsps.activate
    def test_check_results_shows_unmapped_geom_column(self, client):
        rsps.add(rsps.GET, f"{ASYNC_BASE}/check-id-1", json=CHECK_RESULT_COMPLETE, status=200)
        rsps.add(rsps.GET, f"{ASYNC_BASE}/check-id-1/response-details", json=RESP_DETAILS_WITH_GEOM, status=200)
        # Boundary URL fetch is not registered — rsps raises ConnectionError which check.py catches
        with patch("application.blueprints.datamanager.controllers.check.get_organisation_name", return_value="Stockport MBC"):
            with patch("application.blueprints.datamanager.controllers.check.get_dataset_name", return_value="Article 4 Direction Area"):
                response = client.get("/datamanager/check-results/check-id-1")
        assert response.status_code == 200
        assert b"geom" in response.data
        assert b"map[geom]" in response.data  # column mapping select input name

    # --- Step 4: Resubmit with column mapping geom → geometry ---

    @rsps.activate
    def test_column_mapping_resubmit_redirects_to_new_check(self, client):
        rsps.add(rsps.GET, f"{ASYNC_BASE}/check-id-1", json=CHECK_RESULT_COMPLETE, status=200)
        rsps.add(rsps.POST, ASYNC_BASE, json={"id": "check-id-2"}, status=202)
        response = client.post(
            "/datamanager/check-results/check-id-1",
            data={"map[geom]": "geometry"},
        )
        assert response.status_code == 302
        assert "check-results/check-id-2" in response.headers["Location"]

    # --- Step 5: Check Entities button → add_data preview submitted ---

    @rsps.activate
    def test_check_entities_submits_preview_and_redirects(self, client):
        rsps.add(rsps.GET, f"{ASYNC_BASE}/check-id-2", json=RECHECK_RESULT_COMPLETE, status=200)
        rsps.add(rsps.POST, ASYNC_BASE, json={"id": "preview-id-1"}, status=202)
        with client.session_transaction() as sess:
            sess["add_data_fields"] = {
                "documentation_url": "https://www.stockport.gov.uk/planning-development-open-datasets",
                "licence": "ogl3",
                "start_date": "2026-01-22",
                "authoritative": True,
                "column_mapping": {"geom": "geometry"},
            }
        response = client.get("/datamanager/add-data/check-id-2")
        assert response.status_code == 302
        assert "preview-id-1/entities" in response.headers["Location"] or "entities" in response.headers["Location"]

    # --- Step 6: Entities preview — 3 new entities, endpoint not in system ---

    @rsps.activate
    def test_entities_preview_shows_3_new_entities(self, client):
        rsps.add(rsps.GET, f"{ASYNC_BASE}/preview-id-1", json=ENTITIES_PREVIEW_COMPLETE, status=200)
        response = client.get("/datamanager/add-data/preview-id-1/entities")
        assert response.status_code == 200
        assert b"3" in response.data
        assert b"A4a-14-test" in response.data
        assert b"7010009611" in response.data

    @rsps.activate
    def test_entities_preview_endpoint_not_in_system(self, client):
        rsps.add(rsps.GET, f"{ASYNC_BASE}/preview-id-1", json=ENTITIES_PREVIEW_COMPLETE, status=200)
        response = client.get("/datamanager/add-data/preview-id-1/entities")
        assert response.status_code == 200
        assert b"No" in response.data  # endpoint_already_exists = "No"
