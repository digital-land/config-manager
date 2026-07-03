from unittest.mock import patch

import responses as rsps

from application.blueprints.datamanager.controllers.transform import (
    _build_geometry_features,
    _build_entities_data,
    _paginate_entity_data,
)

ASYNC_BASE = "http://localhost:8000/requests"

RESPONSE_DETAILS_URL = "http://localhost:8000/requests/test-id/response-details"

COMPLETED_TRANSFORM_REQUEST = {
    "id": "test-id",
    "status": "COMPLETED",
    "params": {
        "organisationName": "local-authority-eng:ABC",
        "dataset": "conservation-area",
    },
    "response": {
        "data": {
            "source-summary": {},
            "pipeline-summary": {"new-in-resource": 1},
        }
    },
}

RESPONSE_DETAILS = [
    {
        "entry_number": 1,
        "transformed_row": [{"entity": 100, "field": "name", "value": "Area A"}],
        "issue_logs": [],
    },
    {
        "entry_number": 2,
        "transformed_row": [{"entity": 101, "field": "name", "value": "Area B"}],
        "issue_logs": [],
    },
]

PENDING_CHECK_RESULT = {
    "id": "test-id",
    "status": "PENDING",
    "response": None,
    "params": {
        "organisationName": "local-authority-eng:ABC",
        "dataset": "brownfield-land",
    },
}

PENDING_ADD_DATA_RESULT = {
    "id": "test-id",
    "status": "PENDING",
    "response": None,
    "params": {"dataset": "brownfield-land"},
}


class TestDashboardGet:
    def test_returns_200(self, client):
        response = client.get("/datamanager/")
        assert response.status_code == 200

    def test_contains_form(self, client):
        response = client.get("/datamanager/")
        assert b"<form" in response.data

    def test_autocomplete_returns_json(self, client):
        with patch(
            "application.blueprints.datamanager.controllers.form.search_datasets",
            return_value=["brownfield-land"],
        ):
            response = client.get("/datamanager/?autocomplete=brown")
        assert response.status_code == 200
        assert b"brownfield-land" in response.data


class TestImportRoute:
    def test_get_returns_200(self, client):
        response = client.get("/datamanager/import")
        assert response.status_code == 200

    def test_post_with_valid_csv_redirects(self, client):
        csv_data = (
            "organisation,pipelines,endpoint-url\n"
            "local-authority-eng:ABC,brownfield-land,https://example.com/data.csv"
        )
        response = client.post(
            "/datamanager/import", data={"mode": "parse", "csv_data": csv_data}
        )
        assert response.status_code == 302
        assert "import_data=true" in response.headers["Location"]

    def test_post_with_invalid_csv_shows_error(self, client):
        response = client.post(
            "/datamanager/import",
            data={
                "mode": "parse",
                "csv_data": "not,valid,csv\nmissing,required,fields",
            },
        )
        assert response.status_code == 200
        assert b"error" in response.data.lower()


class TestCheckResultsRoute:
    @rsps.activate
    def test_pending_renders_loading(self, client):
        rsps.add(
            rsps.GET, f"{ASYNC_BASE}/test-id", json=PENDING_CHECK_RESULT, status=200
        )
        with patch(
            "application.blueprints.datamanager.controllers.check.get_organisation_name",
            return_value="Test Org",
        ):
            with patch(
                "application.blueprints.datamanager.controllers.check.get_dataset_name",
                return_value="Brownfield Land",
            ):
                response = client.get("/datamanager/check-results/test-id")
        assert response.status_code == 200

    @rsps.activate
    def test_not_found_returns_404(self, client):
        rsps.add(
            rsps.GET,
            f"{ASYNC_BASE}/bad-id",
            json={"detail": {"errMsg": "not found"}},
            status=400,
        )
        response = client.get("/datamanager/check-results/bad-id")
        assert response.status_code == 404

    @rsps.activate
    def test_failed_status_returns_404(self, client):
        rsps.add(
            rsps.GET,
            f"{ASYNC_BASE}/test-id",
            json={**PENDING_CHECK_RESULT, "status": "FAILED"},
            status=200,
        )
        response = client.get("/datamanager/check-results/test-id")
        assert response.status_code == 404


class TestEntitiesPreviewRoute:
    @rsps.activate
    def test_pending_renders_loading(self, client):
        rsps.add(
            rsps.GET, f"{ASYNC_BASE}/test-id", json=PENDING_ADD_DATA_RESULT, status=200
        )
        response = client.get("/datamanager/add-data/test-id/entities")
        assert response.status_code == 200
        assert b"Preparing entities preview" in response.data

    @rsps.activate
    def test_not_found_returns_404(self, client):
        rsps.add(
            rsps.GET,
            f"{ASYNC_BASE}/bad-id",
            json={"detail": {"errMsg": "not found"}},
            status=400,
        )
        response = client.get("/datamanager/add-data/bad-id/entities")
        assert response.status_code == 404

    @rsps.activate
    def test_queued_also_renders_loading(self, client):
        queued = {**PENDING_ADD_DATA_RESULT, "status": "QUEUED"}
        rsps.add(rsps.GET, f"{ASYNC_BASE}/test-id", json=queued, status=200)
        response = client.get("/datamanager/add-data/test-id/entities")
        assert response.status_code == 200
        assert b"Preparing entities preview" in response.data


class TestAddDataConfirmRoute:
    def test_success_renders_success_page(self, client):
        with client.session_transaction() as sess:
            sess["user"] = {"login": "test-user"}
        with patch(
            "application.blueprints.datamanager.controllers.preview.trigger_add_data_async_workflow",
            return_value={"success": True, "message": "Workflow triggered"},
        ):
            response = client.post("/datamanager/add-data/test-id/confirm-async")
        assert response.status_code == 200
        assert b"triggered" in response.data.lower()

    def test_workflow_failure_renders_error(self, client):
        with client.session_transaction() as sess:
            sess["user"] = {"login": "test-user"}
        with patch(
            "application.blueprints.datamanager.controllers.preview.trigger_add_data_async_workflow",
            return_value={"success": False, "message": "Dispatch rejected"},
        ):
            response = client.post("/datamanager/add-data/test-id/confirm-async")
        assert response.status_code == 200
        assert b"govuk-error-summary" in response.data

    def test_github_error_renders_error(self, client):
        from application.blueprints.datamanager.services.github import (
            GitHubWorkflowError,
        )

        with client.session_transaction() as sess:
            sess["user"] = {"login": "test-user"}
        with patch(
            "application.blueprints.datamanager.controllers.preview.trigger_add_data_async_workflow",
            side_effect=GitHubWorkflowError("credentials not configured"),
        ):
            response = client.post("/datamanager/add-data/test-id/confirm-async")
        assert response.status_code == 200
        assert b"govuk-error-summary" in response.data


class TestCheckTransformRoute:
    @rsps.activate
    def test_pending_renders_loading(self, client):
        rsps.add(
            rsps.GET,
            f"{ASYNC_BASE}/test-id",
            json={**COMPLETED_TRANSFORM_REQUEST, "status": "PENDING", "response": None},
            status=200,
        )
        with patch(
            "application.blueprints.datamanager.controllers.transform.get_organisation_name",
            return_value="Test Org",
        ):
            with patch(
                "application.blueprints.datamanager.controllers.transform.get_dataset_name",
                return_value="Conservation Area",
            ):
                response = client.get("/datamanager/check-transform/test-id")
        assert response.status_code == 200
        assert b"Transforming data" in response.data

    @rsps.activate
    def test_failed_status_shows_error(self, client):
        rsps.add(
            rsps.GET,
            f"{ASYNC_BASE}/test-id",
            json={
                **COMPLETED_TRANSFORM_REQUEST,
                "status": "FAILED",
                "response": {"error": {"errMsg": "pipeline error"}},
            },
            status=200,
        )
        with patch(
            "application.blueprints.datamanager.controllers.transform.get_organisation_name",
            return_value="Test Org",
        ):
            with patch(
                "application.blueprints.datamanager.controllers.transform.get_dataset_name",
                return_value="Conservation Area",
            ):
                response = client.get("/datamanager/check-transform/test-id")
        assert response.status_code == 200
        assert b"pipeline error" in response.data

    @rsps.activate
    def test_not_found_returns_404(self, client):
        rsps.add(
            rsps.GET, f"{ASYNC_BASE}/bad-id", json={"detail": "not found"}, status=400
        )
        response = client.get("/datamanager/check-transform/bad-id")
        assert response.status_code == 404

    @rsps.activate
    def test_completed_renders_entities_table(self, client):
        rsps.add(
            rsps.GET,
            f"{ASYNC_BASE}/test-id",
            json=COMPLETED_TRANSFORM_REQUEST,
            status=200,
        )
        rsps.add(rsps.GET, RESPONSE_DETAILS_URL, json=RESPONSE_DETAILS, status=200)
        rsps.add(rsps.GET, RESPONSE_DETAILS_URL, json=[], status=200)
        with patch(
            "application.blueprints.datamanager.controllers.transform.get_organisation_name",
            return_value="Test Org",
        ):
            with patch(
                "application.blueprints.datamanager.controllers.transform.get_dataset_name",
                return_value="Conservation Area",
            ):
                with patch(
                    "application.blueprints.datamanager.controllers.transform.get_org_entity",
                    return_value=None,
                ):
                    response = client.get("/datamanager/check-transform/test-id")
        assert response.status_code == 200
        assert b"entities-table" in response.data

    @rsps.activate
    def test_completed_highlights_new_entities_green(self, client):
        rsps.add(
            rsps.GET,
            f"{ASYNC_BASE}/test-id",
            json=COMPLETED_TRANSFORM_REQUEST,
            status=200,
        )
        rsps.add(rsps.GET, RESPONSE_DETAILS_URL, json=RESPONSE_DETAILS, status=200)
        rsps.add(rsps.GET, RESPONSE_DETAILS_URL, json=[], status=200)
        with patch(
            "application.blueprints.datamanager.controllers.transform.get_organisation_name",
            return_value="Test Org",
        ):
            with patch(
                "application.blueprints.datamanager.controllers.transform.get_dataset_name",
                return_value="Conservation Area",
            ):
                with patch(
                    "application.blueprints.datamanager.controllers.transform.get_org_entity",
                    return_value=None,
                ):
                    response = client.get("/datamanager/check-transform/test-id")
        assert b"#d4edda" in response.data

    @rsps.activate
    def test_completed_highlights_both_entities_orange(self, client):
        rsps.add(
            rsps.GET,
            f"{ASYNC_BASE}/test-id",
            json=COMPLETED_TRANSFORM_REQUEST,
            status=200,
        )
        rsps.add(rsps.GET, RESPONSE_DETAILS_URL, json=RESPONSE_DETAILS, status=200)
        rsps.add(rsps.GET, RESPONSE_DETAILS_URL, json=[], status=200)
        # Different name on the platform → a genuine change → orange "changed" row.
        platform_entities = [{"entity": 100, "name": "Old Area A"}]
        with patch(
            "application.blueprints.datamanager.controllers.transform.get_organisation_name",
            return_value="Test Org",
        ):
            with patch(
                "application.blueprints.datamanager.controllers.transform.get_dataset_name",
                return_value="Conservation Area",
            ):
                with patch(
                    "application.blueprints.datamanager.controllers.transform.get_org_entity",
                    return_value=400,
                ):
                    with patch(
                        "application.blueprints.datamanager.controllers"
                        ".transform.get_entities_for_organisation_and_dataset",
                        return_value=platform_entities,
                    ):
                        response = client.get("/datamanager/check-transform/test-id")
        assert b"#ffd8b0" in response.data

    @rsps.activate
    def test_completed_renders_category_boxes(self, client):
        rsps.add(
            rsps.GET,
            f"{ASYNC_BASE}/test-id",
            json=COMPLETED_TRANSFORM_REQUEST,
            status=200,
        )
        rsps.add(rsps.GET, RESPONSE_DETAILS_URL, json=RESPONSE_DETAILS, status=200)
        rsps.add(rsps.GET, RESPONSE_DETAILS_URL, json=[], status=200)
        with patch(
            "application.blueprints.datamanager.controllers.transform.get_organisation_name",
            return_value="Test Org",
        ):
            with patch(
                "application.blueprints.datamanager.controllers.transform.get_dataset_name",
                return_value="Conservation Area",
            ):
                with patch(
                    "application.blueprints.datamanager.controllers.transform.get_org_entity",
                    return_value=None,
                ):
                    response = client.get("/datamanager/check-transform/test-id")
        assert b"app-stat-box" in response.data
        # Two resource-only entities in RESPONSE_DETAILS → New count of 2.
        assert b"Matching platform" in response.data
        assert b"Platform only" in response.data

    @rsps.activate
    def test_changed_cell_highlighted_with_platform_value(self, client):
        rsps.add(
            rsps.GET,
            f"{ASYNC_BASE}/test-id",
            json=COMPLETED_TRANSFORM_REQUEST,
            status=200,
        )
        rsps.add(rsps.GET, RESPONSE_DETAILS_URL, json=RESPONSE_DETAILS, status=200)
        rsps.add(rsps.GET, RESPONSE_DETAILS_URL, json=[], status=200)
        platform_entities = [{"entity": 100, "name": "Old Name"}]
        with patch(
            "application.blueprints.datamanager.controllers.transform.get_organisation_name",
            return_value="Test Org",
        ):
            with patch(
                "application.blueprints.datamanager.controllers.transform.get_dataset_name",
                return_value="Conservation Area",
            ):
                with patch(
                    "application.blueprints.datamanager.controllers.transform.get_org_entity",
                    return_value=400,
                ):
                    with patch(
                        "application.blueprints.datamanager.controllers"
                        ".transform.get_entities_for_organisation_and_dataset",
                        return_value=platform_entities,
                    ):
                        response = client.get("/datamanager/check-transform/test-id")
        assert b"app-cell-changed" in response.data
        assert b"Platform value: Old Name" in response.data
        assert b'data-platform-value="Platform value: Old Name"' in response.data
        # Category filter dropdown is rendered on the entities table.
        assert b'name="entity_filter"' in response.data

    @rsps.activate
    def test_map_renders_with_no_platform_entities(self, client):
        details = [
            {
                "entry_number": 1,
                "converted_row": {"reference": "R1", "name": "Area A"},
                "transformed_row": [
                    {"entity": 100, "field": "name", "value": "Area A"},
                    {"entity": 100, "field": "geometry", "value": "POINT (-2.5 54.5)"},
                ],
                "issue_logs": [],
            }
        ]
        # fetch_response_details is memoized by request_id, so use a distinct
        # id to avoid the cached details from other tests.
        rsps.add(
            rsps.GET,
            f"{ASYNC_BASE}/geo-test-id",
            json={**COMPLETED_TRANSFORM_REQUEST, "id": "geo-test-id"},
            status=200,
        )
        geo_details_url = f"{ASYNC_BASE}/geo-test-id/response-details"
        rsps.add(rsps.GET, geo_details_url, json=details, status=200)
        rsps.add(rsps.GET, geo_details_url, json=[], status=200)
        with patch(
            "application.blueprints.datamanager.controllers.transform.get_organisation_name",
            return_value="Test Org",
        ):
            with patch(
                "application.blueprints.datamanager.controllers.transform.get_dataset_name",
                return_value="Conservation Area",
            ):
                with patch(
                    "application.blueprints.datamanager.controllers.transform.get_org_entity",
                    return_value=None,
                ):
                    with patch(
                        "application.blueprints.datamanager.controllers.transform.get_dataset_typology",
                        return_value="geography",
                    ):
                        response = client.get(
                            "/datamanager/check-transform/geo-test-id"
                        )
        assert response.status_code == 200
        assert b"map-container" in response.data
        # Representative points are passed to the clustered map source.
        assert b"geometryPoints" in response.data

    @rsps.activate
    def test_endpoint_url_shown_at_top(self, client):
        request_json = {
            **COMPLETED_TRANSFORM_REQUEST,
            "params": {
                **COMPLETED_TRANSFORM_REQUEST["params"],
                "url": "https://example.com/data.csv",
                "documentation_url": "https://example.gov.uk/docs",
            },
        }
        rsps.add(rsps.GET, f"{ASYNC_BASE}/test-id", json=request_json, status=200)
        rsps.add(rsps.GET, RESPONSE_DETAILS_URL, json=RESPONSE_DETAILS, status=200)
        rsps.add(rsps.GET, RESPONSE_DETAILS_URL, json=[], status=200)
        with patch(
            "application.blueprints.datamanager.controllers.transform.get_organisation_name",
            return_value="Test Org",
        ):
            with patch(
                "application.blueprints.datamanager.controllers.transform.get_dataset_name",
                return_value="Conservation Area",
            ):
                with patch(
                    "application.blueprints.datamanager.controllers.transform.get_org_entity",
                    return_value=None,
                ):
                    with patch(
                        "application.blueprints.datamanager.controllers"
                        ".transform.check_endpoint_in_doc",
                        return_value={
                            "found": False,
                            "matched_href": None,
                            "error": None,
                        },
                    ):
                        response = client.get("/datamanager/check-transform/test-id")
        assert b"Endpoint URL" in response.data
        assert (
            b'<a class="govuk-link" href="https://example.com/data.csv"'
            in response.data
        )

    @rsps.activate
    def test_completed_page_title_is_dataset_first(self, client):
        rsps.add(
            rsps.GET,
            f"{ASYNC_BASE}/test-id",
            json=COMPLETED_TRANSFORM_REQUEST,
            status=200,
        )
        rsps.add(rsps.GET, RESPONSE_DETAILS_URL, json=RESPONSE_DETAILS, status=200)
        rsps.add(rsps.GET, RESPONSE_DETAILS_URL, json=[], status=200)
        with patch(
            "application.blueprints.datamanager.controllers.transform.get_organisation_name",
            return_value="Test Org",
        ):
            with patch(
                "application.blueprints.datamanager.controllers.transform.get_dataset_name",
                return_value="Conservation Area",
            ):
                with patch(
                    "application.blueprints.datamanager.controllers.transform.get_org_entity",
                    return_value=None,
                ):
                    response = client.get("/datamanager/check-transform/test-id")
        assert (
            "Conservation Area – Test Org – Provision Comparison".encode("utf-8")
            in response.data
        )

    @rsps.activate
    def test_loading_page_title_names_dataset(self, client):
        rsps.add(
            rsps.GET,
            f"{ASYNC_BASE}/test-id",
            json={**COMPLETED_TRANSFORM_REQUEST, "status": "PENDING", "response": None},
            status=200,
        )
        with patch(
            "application.blueprints.datamanager.controllers.transform.get_organisation_name",
            return_value="Test Org",
        ):
            with patch(
                "application.blueprints.datamanager.controllers.transform.get_dataset_name",
                return_value="Conservation Area",
            ):
                response = client.get("/datamanager/check-transform/test-id")
        assert (
            "Transforming data – Conservation Area – Test Org".encode("utf-8")
            in response.data
        )


class TestBuildEntitiesData:
    def _make_detail(self, entity, field, value):
        return {
            "entry_number": 1,
            "transformed_row": [{"entity": entity, "field": field, "value": value}],
            "issue_logs": [],
        }

    def test_entity_only_in_resource_is_new(self):
        details = [self._make_detail(101, "name", "Area B")]
        result = _build_entities_data(details, [{"entity": 100, "name": "Area A"}])
        row = next(r for r in result["rows"] if r["fields"]["entity"] == "101")
        assert row["category"] == "new"

    def test_entity_in_both_is_flagged(self):
        details = [self._make_detail(100, "name", "Area A Updated")]
        result = _build_entities_data(details, [{"entity": 100, "name": "Area A"}])
        row = next(r for r in result["rows"] if r["fields"]["entity"] == "100")
        assert row["category"] == "changed"

    def test_entity_only_on_platform_not_new(self):
        result = _build_entities_data([], [{"entity": 100, "name": "Area A"}])
        row = next(r for r in result["rows"] if r["fields"]["entity"] == "100")
        assert row["category"] == "existing"

    def test_float_entity_id_matches_platform_integer(self):
        details = [self._make_detail(44015862.0, "name", "Lydford Updated")]
        result = _build_entities_data(
            details, [{"entity": 44015862, "name": "Lydford"}]
        )
        row = next(r for r in result["rows"] if r["fields"]["entity"] == "44015862")
        assert row["category"] == "changed"

    def test_platform_only_entity_appended_to_rows(self):
        result = _build_entities_data([], [{"entity": 999, "name": "Only Platform"}])
        assert any(r["fields"]["entity"] == "999" for r in result["rows"])

    def test_platform_only_rows_appended_after_resource_rows(self):
        details = [self._make_detail(200, "name", "Resource Entity")]
        platform = [{"entity": 999, "name": "Platform Only"}]
        result = _build_entities_data(details, platform)
        entities = [r["fields"]["entity"] for r in result["rows"]]
        assert entities.index("200") < entities.index("999")

    def test_total_row_count_includes_resource_and_platform_only(self):
        details = [self._make_detail(i, "name", f"Area {i}") for i in range(10)]
        platform = [{"entity": 100 + i, "name": f"Platform {i}"} for i in range(5)]
        result = _build_entities_data(details, platform)
        assert len(result["rows"]) == 15

    def test_in_both_row_flags_changed_fields(self):
        details = [self._make_detail(100, "name", "Area A Updated")]
        result = _build_entities_data(details, [{"entity": 100, "name": "Area A"}])
        row = next(r for r in result["rows"] if r["fields"]["entity"] == "100")
        assert row["changed_fields"] == {"name": "Area A"}

    def test_in_both_row_with_equal_values_has_no_changed_fields(self):
        details = [self._make_detail(100, "name", "Area A")]
        result = _build_entities_data(details, [{"entity": 100, "name": "Area A"}])
        row = next(r for r in result["rows"] if r["fields"]["entity"] == "100")
        assert row["changed_fields"] == {}

    def test_new_and_platform_only_rows_have_empty_changed_fields(self):
        details = [self._make_detail(101, "name", "Area B")]
        result = _build_entities_data(details, [{"entity": 100, "name": "Area A"}])
        for row in result["rows"]:
            assert row["changed_fields"] == {}

    def test_numeric_values_normalised_before_comparison(self):
        details = [self._make_detail(100, "reference", "12.0")]
        result = _build_entities_data(details, [{"entity": 100, "reference": 12}])
        row = next(r for r in result["rows"] if r["fields"]["entity"] == "100")
        assert row["changed_fields"] == {}

    def test_datetime_values_compared_on_date_part(self):
        details = [self._make_detail(100, "start-date", "2024-01-01")]
        result = _build_entities_data(
            details, [{"entity": 100, "start-date": "2024-01-01T00:00:00Z"}]
        )
        row = next(r for r in result["rows"] if r["fields"]["entity"] == "100")
        assert row["changed_fields"] == {}

    def test_platform_only_column_not_flagged(self):
        details = [self._make_detail(100, "name", "Area A")]
        result = _build_entities_data(
            details,
            [{"entity": 100, "name": "Area A", "entry-date": "2024-01-01"}],
        )
        row = next(r for r in result["rows"] if r["fields"]["entity"] == "100")
        assert row["changed_fields"] == {}

    def test_differing_geometry_text_not_flagged(self):
        details = [self._make_detail(100, "geometry", "POINT (1 2)")]
        result = _build_entities_data(
            details,
            [{"entity": 100, "geometry": "MULTIPOINT ((1.000000 2.000000))"}],
        )
        row = next(r for r in result["rows"] if r["fields"]["entity"] == "100")
        assert row["changed_fields"] == {}

    def test_geometry_presence_mismatch_flagged(self):
        details = [self._make_detail(100, "geometry", "POINT (1 2)")]
        result = _build_entities_data(details, [{"entity": 100, "name": "Area A"}])
        row = next(r for r in result["rows"] if r["fields"]["entity"] == "100")
        assert "geometry" in row["changed_fields"]

    def test_moved_geometry_flagged(self):
        details = [self._make_detail(100, "geometry", "POINT (1 2)")]
        result = _build_entities_data(
            details, [{"entity": 100, "geometry": "POINT (5 6)"}]
        )
        row = next(r for r in result["rows"] if r["fields"]["entity"] == "100")
        assert "geometry" in row["changed_fields"]
        assert row["category"] == "changed"

    def test_dropped_value_flagged_with_platform_value(self):
        details = [self._make_detail(100, "name", "")]
        result = _build_entities_data(details, [{"entity": 100, "name": "Area A"}])
        row = next(r for r in result["rows"] if r["fields"]["entity"] == "100")
        assert row["changed_fields"] == {"name": "Area A"}

    def test_float_platform_id_not_duplicated_as_platform_only_row(self):
        details = [self._make_detail(100, "name", "Area A Updated")]
        result = _build_entities_data(details, [{"entity": 100.0, "name": "Area A"}])
        matching = [r for r in result["rows"] if r["fields"]["entity"] == "100"]
        assert len(matching) == 1
        assert matching[0]["category"] == "changed"

    def test_in_both_unchanged_is_in_both_category(self):
        details = [self._make_detail(100, "name", "Area A")]
        result = _build_entities_data(details, [{"entity": 100, "name": "Area A"}])
        row = next(r for r in result["rows"] if r["fields"]["entity"] == "100")
        assert row["category"] == "in_both"
        assert row["changed_fields"] == {}

    def test_row_categories_new_changed_in_both_existing(self):
        details = [
            self._make_detail(100, "name", "New Area"),  # resource only
            self._make_detail(200, "name", "Changed"),  # in both, changed
            self._make_detail(300, "name", "Same"),  # in both, unchanged
        ]
        platform = [
            {"entity": 200, "name": "Original"},
            {"entity": 300, "name": "Same"},
            {"entity": 400, "name": "Platform Only"},  # existing
        ]
        result = _build_entities_data(details, platform)
        by_id = {r["fields"]["entity"]: r["category"] for r in result["rows"]}
        assert by_id["100"] == "new"
        assert by_id["200"] == "changed"
        assert by_id["300"] == "in_both"
        assert by_id["400"] == "existing"


class TestEntityFilter:
    def _make_detail(self, entity, field, value):
        return {
            "entry_number": 1,
            "transformed_row": [{"entity": entity, "field": field, "value": value}],
            "issue_logs": [],
        }

    def test_filter_returns_only_matching_category(self):
        details = [
            self._make_detail(100, "name", "New Area"),  # new
            self._make_detail(200, "name", "Changed"),  # changed
            self._make_detail(300, "name", "Same"),  # in_both
        ]
        platform = [
            {"entity": 200, "name": "Original"},
            {"entity": 300, "name": "Same"},
            {"entity": 400, "name": "Platform Only"},  # existing
        ]
        entities_data, _, _, _, _ = _paginate_entity_data(
            details, platform, entity_page=1, entity_search="", entity_filter="changed"
        )
        ids = [r["fields"]["entity"] for r in entities_data["rows"]]
        assert ids == ["200"]

    def test_no_filter_returns_all_rows(self):
        details = [self._make_detail(100, "name", "New Area")]
        platform = [{"entity": 400, "name": "Platform Only"}]
        entities_data, _, _, _, _ = _paginate_entity_data(
            details, platform, entity_page=1, entity_search="", entity_filter=""
        )
        assert len(entities_data["rows"]) == 2

    def test_category_counts_returned(self):
        details = [
            self._make_detail(100, "name", "New Area"),  # new
            self._make_detail(200, "name", "Changed"),  # changed
            self._make_detail(300, "name", "Same"),  # in_both
        ]
        platform = [
            {"entity": 200, "name": "Original"},
            {"entity": 300, "name": "Same"},
            {"entity": 400, "name": "Platform Only"},  # existing
        ]
        *_, category_counts = _paginate_entity_data(
            details, platform, entity_page=1, entity_search="", entity_filter=""
        )
        assert category_counts == {
            "new": 1,
            "changed": 1,
            "in_both": 1,
            "existing": 1,
        }


_GEOGRAPHY_TYPOLOGY_PATCH = patch(
    "application.blueprints.datamanager.controllers.transform.get_dataset_typology",
    return_value="geography",
)


class TestBuildGeometryFeatures:
    def _geometry_detail(self, entity, wkt_value):
        return {
            "entry_number": 1,
            "converted_row": {"reference": "R1", "name": "Area A"},
            "transformed_row": [
                {"entity": entity, "field": "name", "value": "Area A"},
                {"entity": entity, "field": "geometry", "value": wkt_value},
            ],
            "issue_logs": [],
        }

    def _polygon_detail(self, entity, wkt_value):
        return {
            "entry_number": 1,
            "converted_row": {"reference": "R1", "name": "Area A"},
            "transformed_row": [
                {"entity": entity, "field": "name", "value": "Area A"},
                {"entity": entity, "field": "geometry", "value": wkt_value},
            ],
            "issue_logs": [],
        }

    def test_resource_geometry_with_no_platform_entities_is_new(self):
        details = [self._geometry_detail(100, "POINT (-2.5 54.5)")]
        with _GEOGRAPHY_TYPOLOGY_PATCH:
            features, points = _build_geometry_features(
                [], details, "article-4-direction-area"
            )
        assert len(features) == 1
        assert features[0]["properties"]["status"] == "new"
        assert len(points) == 1
        assert points[0]["geometry"]["type"] == "Point"
        assert points[0]["properties"]["status"] == "new"

    def test_resource_geometry_unchanged_is_in_both(self):
        details = [self._geometry_detail(100, "POINT (-2.5 54.5)")]
        platform = [{"entity": 100, "name": "Area A", "geometry": "POINT (-2.5 54.5)"}]
        with _GEOGRAPHY_TYPOLOGY_PATCH:
            features, _ = _build_geometry_features(
                platform, details, "article-4-direction-area"
            )
        statuses = {f["properties"]["status"] for f in features}
        assert statuses == {"in_both"}

    def test_resource_geometry_moved_is_changed(self):
        details = [self._geometry_detail(100, "POINT (-2.5 54.5)")]
        platform = [{"entity": 100, "name": "Area A", "geometry": "POINT (-3.0 55.0)"}]
        with _GEOGRAPHY_TYPOLOGY_PATCH:
            features, _ = _build_geometry_features(
                platform, details, "article-4-direction-area"
            )
        statuses = {f["properties"]["status"] for f in features}
        assert statuses == {"changed"}

    def test_no_geometry_in_rows_returns_empty(self):
        details = [
            {
                "entry_number": 1,
                "converted_row": {},
                "transformed_row": [{"entity": 100, "field": "name", "value": "A"}],
                "issue_logs": [],
            }
        ]
        with _GEOGRAPHY_TYPOLOGY_PATCH:
            features, points = _build_geometry_features(
                [{"entity": 200, "name": "B"}], details, "article-4-direction-area"
            )
        assert features == []
        assert points == []

    def test_polygon_entity_point_is_inside_and_flagged(self):
        # A square around (0,0); representative point must fall inside it.
        square = "POLYGON ((-1 -1, 1 -1, 1 1, -1 1, -1 -1))"
        details = [self._polygon_detail(100, square)]
        with _GEOGRAPHY_TYPOLOGY_PATCH:
            _, points = _build_geometry_features(
                [], details, "article-4-direction-area"
            )
        assert len(points) == 1
        assert points[0]["properties"]["has_polygon"] is True
        lon, lat = points[0]["geometry"]["coordinates"]
        assert -1 <= lon <= 1 and -1 <= lat <= 1

    def test_explicit_point_field_used_for_marker(self):
        square = "POLYGON ((-1 -1, 1 -1, 1 1, -1 1, -1 -1))"
        details = [
            {
                "entry_number": 1,
                "converted_row": {"reference": "R1"},
                "transformed_row": [
                    {"entity": 100, "field": "geometry", "value": square},
                    {"entity": 100, "field": "point", "value": "POINT (0.5 0.25)"},
                ],
                "issue_logs": [],
            }
        ]
        with _GEOGRAPHY_TYPOLOGY_PATCH:
            _, points = _build_geometry_features(
                [], details, "article-4-direction-area"
            )
        assert points[0]["geometry"]["coordinates"] == [0.5, 0.25]
        assert points[0]["properties"]["has_polygon"] is True

    def test_point_only_entity_not_flagged_as_polygon(self):
        details = [
            {
                "entry_number": 1,
                "converted_row": {"reference": "R1"},
                "transformed_row": [
                    {"entity": 100, "field": "point", "value": "POINT (-2.5 54.5)"},
                ],
                "issue_logs": [],
            }
        ]
        with _GEOGRAPHY_TYPOLOGY_PATCH:
            _, points = _build_geometry_features(
                [], details, "article-4-direction-area"
            )
        assert points[0]["properties"]["has_polygon"] is False


class TestEntityPagination:
    """Entity table paginates over the full combined set (resource + platform-only)
    independently of the transform/issue log page_number param."""

    def _make_details(self, count, start_entity=7010000001):
        return [
            {
                "entry_number": i,
                "transformed_row": [
                    {
                        "entity": start_entity + i,
                        "field": "name",
                        "value": f"Area {start_entity + i}",
                    }
                ],
                "issue_logs": [],
            }
            for i in range(count)
        ]

    @rsps.activate
    def test_entity_colours_new_both_and_platform_only(self, client):
        """Entity 123 (resource only) = green, 125 (both) = orange, 124 (platform only) = no highlight."""
        details = [
            {
                "entry_number": 1,
                "transformed_row": [
                    {"entity": 123, "field": "name", "value": "New Area"}
                ],
                "issue_logs": [],
            },
            {
                "entry_number": 2,
                "transformed_row": [
                    {"entity": 125, "field": "name", "value": "Shared Area Updated"}
                ],
                "issue_logs": [],
            },
        ]
        platform_entities = [
            {"entity": 124, "name": "Platform Only Area"},
            {"entity": 125, "name": "Shared Area"},
        ]
        rsps.add(
            rsps.GET,
            f"{ASYNC_BASE}/test-id",
            json=COMPLETED_TRANSFORM_REQUEST,
            status=200,
        )
        with patch(
            "application.blueprints.datamanager.controllers.transform.fetch_response_details",
            return_value=details,
        ):
            with patch(
                "application.blueprints.datamanager.controllers.transform.get_organisation_name",
                return_value="Test Org",
            ):
                with patch(
                    "application.blueprints.datamanager.controllers.transform.get_dataset_name",
                    return_value="Conservation Area",
                ):
                    with patch(
                        "application.blueprints.datamanager.controllers.transform.get_org_entity",
                        return_value=400,
                    ):
                        with patch(
                            "application.blueprints.datamanager.controllers"
                            ".transform.get_entity_count_for_organisation_and_dataset",
                            return_value=2,
                        ):
                            with patch(
                                "application.blueprints.datamanager.controllers"
                                ".transform.get_entities_for_organisation_and_dataset",
                                return_value=platform_entities,
                            ):
                                response = client.get(
                                    "/datamanager/check-transform/test-id"
                                )
        html = response.data.decode()
        # Entity 123 (resource only) row should be green
        assert "#d4edda" in html
        # Entity 125 (in both, changed) row should be orange
        assert "#ffd8b0" in html
        # Entity 124 (platform only) appears but with no highlight colour
        assert "Platform Only Area" in html
        rows_with_colour = [
            line for line in html.splitlines() if "#d4edda" in line or "#ffd8b0" in line
        ]
        assert not any("Platform Only Area" in line for line in rows_with_colour)

    @rsps.activate
    def test_entity_page_2_shows_later_entities_not_on_page_1(self, client):
        details = self._make_details(600)
        rsps.add(
            rsps.GET,
            f"{ASYNC_BASE}/test-id",
            json=COMPLETED_TRANSFORM_REQUEST,
            status=200,
        )
        with patch(
            "application.blueprints.datamanager.controllers.transform.fetch_response_details",
            return_value=details,
        ):
            with patch(
                "application.blueprints.datamanager.controllers.transform.get_organisation_name",
                return_value="Test Org",
            ):
                with patch(
                    "application.blueprints.datamanager.controllers.transform.get_dataset_name",
                    return_value="Conservation Area",
                ):
                    with patch(
                        "application.blueprints.datamanager.controllers.transform.get_org_entity",
                        return_value=None,
                    ):
                        resp1 = client.get(
                            "/datamanager/check-transform/test-id?entity_page=1"
                        )
                        resp2 = client.get(
                            "/datamanager/check-transform/test-id?entity_page=2"
                        )
        # "Showing entities X to Y" text is specific to the entities tab
        assert b"Showing entities 7010000001" in resp1.data
        assert b"Showing entities 7010000501" in resp2.data
        assert b"Showing entities 7010000501" not in resp1.data

    @rsps.activate
    def test_platform_only_entities_reachable_beyond_resp_details_count(self, client):
        """Platform-only entities push total entity rows past _ROWS_PER_PAGE so they are
        reachable via entity_page even though resp_details is small."""
        details = self._make_details(10, start_entity=7010000001)
        platform_entities = [
            {"entity": 8000000000 + i, "name": f"Platform {8000000000 + i}"}
            for i in range(600)
        ]
        rsps.add(
            rsps.GET,
            f"{ASYNC_BASE}/test-id",
            json=COMPLETED_TRANSFORM_REQUEST,
            status=200,
        )
        with patch(
            "application.blueprints.datamanager.controllers.transform.fetch_response_details",
            return_value=details,
        ):
            with patch(
                "application.blueprints.datamanager.controllers.transform.get_organisation_name",
                return_value="Test Org",
            ):
                with patch(
                    "application.blueprints.datamanager.controllers.transform.get_dataset_name",
                    return_value="Conservation Area",
                ):
                    with patch(
                        "application.blueprints.datamanager.controllers.transform.get_org_entity",
                        return_value=400,
                    ):
                        with patch(
                            "application.blueprints.datamanager.controllers"
                            ".transform.get_entity_count_for_organisation_and_dataset",
                            return_value=600,
                        ):
                            with patch(
                                "application.blueprints.datamanager.controllers"
                                ".transform.get_entities_for_organisation_and_dataset",
                                return_value=platform_entities,
                            ):
                                resp1 = client.get(
                                    "/datamanager/check-transform/test-id?entity_page=1"
                                )
                                resp2 = client.get(
                                    "/datamanager/check-transform/test-id?entity_page=2"
                                )
        # page 1: 10 resource + 490 platform-only (8000000000–8000000489); page 2: remaining 110
        assert b"Showing entities 7010000001 to 8000000489" in resp1.data
        assert b"Showing entities 8000000490" in resp2.data
        assert b"Showing entities 8000000490" not in resp1.data
        # page 1 must have a next link pointing to entity_page=2
        assert b"entity_page=2" in resp1.data

    @rsps.activate
    def test_entity_showing_text_uses_entity_ids(self, client):
        details = self._make_details(10, start_entity=7010000100)
        rsps.add(
            rsps.GET,
            f"{ASYNC_BASE}/test-id",
            json=COMPLETED_TRANSFORM_REQUEST,
            status=200,
        )
        with patch(
            "application.blueprints.datamanager.controllers.transform.fetch_response_details",
            return_value=details,
        ):
            with patch(
                "application.blueprints.datamanager.controllers.transform.get_organisation_name",
                return_value="Test Org",
            ):
                with patch(
                    "application.blueprints.datamanager.controllers.transform.get_dataset_name",
                    return_value="Conservation Area",
                ):
                    with patch(
                        "application.blueprints.datamanager.controllers.transform.get_org_entity",
                        return_value=None,
                    ):
                        response = client.get("/datamanager/check-transform/test-id")
        assert b"Showing entities 7010000100 to 7010000109" in response.data
