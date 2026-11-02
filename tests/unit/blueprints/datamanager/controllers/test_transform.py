import json
from unittest.mock import patch

from application.utils import compute_hash
from application.blueprints.datamanager.controllers.transform import (
    _dedup_candidate_form_value,
    _build_geometry_features,
    _dedup_dynamic_columns,
    _fetch_platform_entities,
    _prepare_duplicate_candidates,
    _resolve_existing_endpoints,
    _show_dedup_tab,
)
from application.blueprints.datamanager.services.planning_data import (
    PlatformEntitiesIncomplete,
)

TRANSFORM_MODULE = "application.blueprints.datamanager.controllers.transform"


def test_dedup_candidate_form_value_builds_redirect_payload():
    form_value = _dedup_candidate_form_value(
        {
            "old_entity": "100",
            "entity": "200",
            "dataset": "conservation-area",
            "old_reference": "old-ref",
            "new_reference": "new-ref",
            "match_type": "complete_match",
        }
    )

    assert json.loads(form_value) == {
        "old_entity": "100",
        "dataset": "conservation-area",
        "new_reference": "new-ref",
        "status": "",
    }


def test_prepare_duplicate_candidates_does_not_auto_select_complete_matches_without_old_entity():
    candidates = _prepare_duplicate_candidates(
        [
            {
                "old_entity": "100",
                "entity": "200",
                "match_type": "complete_match",
                "name_similarity": 10,
            }
        ]
    )

    assert candidates[0]["auto_select"] is False


def test_prepare_duplicate_candidates_sorts_by_old_then_new_entity():
    candidates = _prepare_duplicate_candidates(
        [
            {"old_entity": "10", "entity": "100"},
            {"old_entity": "2", "entity": "100"},
            {"old_entity": "10", "entity": "2"},
            {"old_entity": "2", "entity": "2"},
        ]
    )

    assert [
        (candidate["old_entity"], candidate["entity"]) for candidate in candidates
    ] == [
        ("2", "2"),
        ("2", "100"),
        ("10", "2"),
        ("10", "100"),
    ]


def test_prepare_duplicate_candidates_auto_selects_old_entity_rows():
    candidates = _prepare_duplicate_candidates(
        [
            {
                "old_entity": "100",
                "entity": "200",
                "match_type": "complete_match",
                "old_entity_redirects": [
                    {"old-entity": "100", "entity": "300", "status": "301"}
                ],
            }
        ],
        [{"old-entity": "100", "entity": "200", "status": "301"}],
    )

    assert candidates[0]["auto_select"] is True
    assert candidates[0]["redirect_locked"] is True
    assert candidates[0]["redirect_can_select"] is True
    assert candidates[0]["redirect_status"] == "301"


def test_prepare_duplicate_candidates_does_not_lock_selected_redirect_rows():
    candidates = _prepare_duplicate_candidates(
        [
            {
                "old_entity": "100",
                "entity": "200",
                "match_type": "complete_match",
                "new_reference": "ref-1",
            }
        ],
        [
            {
                "old-entity": "100",
                "entity": "200",
                "status": "301",
            }
        ],
        selected_redirects=[
            {
                "reference": "ref-1",
                "old_entity_number": "100",
                "status": "410",
            }
        ],
    )

    assert candidates[0]["auto_select"] is True
    assert candidates[0]["redirect_selected"] is True
    assert candidates[0]["redirect_locked"] is False
    assert candidates[0]["redirect_status"] == "410"
    assert json.loads(candidates[0]["form_value"])["status"] == "410"
    assert "entity" not in json.loads(candidates[0]["form_value"])


def test_prepare_duplicate_candidates_does_not_auto_select_unmatched_old_entity_rows():
    candidates = _prepare_duplicate_candidates(
        [
            {
                "old_entity": "100",
                "entity": "200",
                "match_type": "single_match",
                "name_similarity": 86,
            }
        ],
        [{"old-entity": "101", "entity": "200", "status": "301"}],
    )

    assert candidates[0]["auto_select"] is False


def test_prepare_duplicate_candidates_keeps_old_entity_field_alias():
    candidates = _prepare_duplicate_candidates(
        [
            {
                "old_entity": "100",
                "entity": "200",
                "match_type": "single_match",
                "name_similarity": 85,
            }
        ],
        [{"old_entity": "100", "entity": "200", "status": "301"}],
    )

    assert candidates[0]["auto_select"] is True


def test_prepare_duplicate_candidates_disables_redirects_for_excluded_references():
    candidates = _prepare_duplicate_candidates(
        [
            {
                "old_entity": "100",
                "entity": "200",
                "new_reference": "ref-1",
            },
            {
                "old_entity": "101",
                "entity": "201",
                "new_reference": "ref-2",
            },
        ],
        excluded_references=["ref-1"],
    )

    assert candidates[0]["redirect_can_select"] is False
    assert candidates[1]["redirect_can_select"] is True


def test_resolve_existing_endpoints_enriches_sorts_and_flags():
    current_url = "https://example.com/current.csv"
    current_hash = compute_hash(current_url)
    source_summary = {
        "existing_endpoint_for_organisation_dataset": [
            "hash-old",
            current_hash,
            "hash-new",
        ]
    }
    endpoint_data = {
        "hash-old": {
            "endpoint_url": "https://example.com/old.csv",
            "entry_date": "2026-01-01",
            "end_date": "2026-06-01",
        },
        current_hash: {
            "endpoint_url": current_url,
            "entry_date": "2026-03-01",
            "end_date": "",
        },
        "hash-new": {
            "endpoint_url": "https://example.com/new.csv",
            "entry_date": "2026-05-01",
            "end_date": "",
        },
    }
    log_data = {
        "hash-new": {
            "latest_status": "200",
            "latest_log_entry_date": "2026-07-20",
        }
    }

    with patch(
        f"{TRANSFORM_MODULE}.get_endpoint_info_for_hashes", return_value=endpoint_data
    ), patch(
        f"{TRANSFORM_MODULE}.get_endpoint_log_summary_for_hashes", return_value=log_data
    ):
        result = _resolve_existing_endpoints(source_summary, current_url)

    # Sorted by entry-date desc: hash-new (05-01), current (03-01), hash-old (01-01)
    assert [r["endpoint"] for r in result] == ["hash-new", current_hash, "hash-old"]

    by_hash = {r["endpoint"]: r for r in result}
    assert by_hash["hash-old"]["is_retired"] is True
    assert by_hash["hash-old"]["is_current"] is False
    assert by_hash[current_hash]["is_current"] is True
    assert by_hash["hash-new"]["latest-status"] == "200"
    assert by_hash["hash-new"]["latest-log-entry-date"] == "2026-07-20"


def test_prepare_duplicate_candidates_classifies_redirect_targets_by_entity_number():
    candidates = _prepare_duplicate_candidates(
        [
            {"old_entity": "100", "entity": "200", "new_reference": "existing"},
            {"old_entity": "101", "entity": "201", "new_reference": "new"},
            {"old_entity": "102", "entity": "202", "new_reference": "unknown"},
        ],
        new_entity_rows=[{"entity": "201", "reference": "new"}],
        existing_entity_rows=[{"entity": "200", "reference": "existing"}],
    )

    assert candidates[0]["redirect_can_select"] is True
    assert candidates[0]["target_requires_assignment"] is False
    assert candidates[1]["redirect_can_select"] is True
    assert candidates[1]["target_requires_assignment"] is True
    assert candidates[2]["redirect_can_select"] is False
    assert candidates[2]["target_requires_assignment"] is False


def test_prepare_duplicate_candidates_keeps_generic_field_maps_and_columns():
    candidates = _prepare_duplicate_candidates(
        [
            {
                "old_entity": "100",
                "entity": "200",
                "dataset": "tree-preservation-order",
                "old_reference": "old-ref",
                "new_reference": "new-ref",
                "old_fields": {
                    "reference": "old-ref",
                    "name": "Old name",
                    "category": "Old category",
                    "dataset": "tree-preservation-order",
                },
                "new_fields": {
                    "reference": "new-ref",
                    "name": "New name",
                    "category": "New category",
                    "dataset": "tree-preservation-order",
                },
            }
        ]
    )

    assert candidates[0]["auto_select"] is False
    assert candidates[0]["old_fields"]["category"] == "Old category"
    assert candidates[0]["new_fields"]["category"] == "New category"
    assert _dedup_dynamic_columns(candidates) == ["category"]


def test_show_dedup_tab_for_all_datasets_when_assigning_entities():
    assert _show_dedup_tab(True, "tree-preservation-order", "legal-instrument")
    assert _show_dedup_tab(True, "conservation-area", "geography")
    assert _show_dedup_tab(True, "article-4-direction-area", "geography")
    assert not _show_dedup_tab(False, "tree-preservation-order", "legal-instrument")
    assert not _show_dedup_tab(False, "article-4-direction-area", "geography")
    assert not _show_dedup_tab(False, "conservation-area", "geography")


class TestFetchPlatformEntitiesFailure:
    def test_incomplete_platform_fetch_reports_failure_and_no_entities(self, app):
        # A partial platform list would report existing entities as new, so the
        # controller must get nothing back plus a flag to suppress the comparison.
        with app.app_context():
            with patch(f"{TRANSFORM_MODULE}.get_org_entity", return_value=400):
                with patch(
                    f"{TRANSFORM_MODULE}.get_entity_count_for_organisation_and_dataset",
                    return_value=800,
                ):
                    with patch(
                        f"{TRANSFORM_MODULE}.get_entities_for_organisation_and_dataset",
                        side_effect=PlatformEntitiesIncomplete("page 2 failed"),
                    ):
                        (
                            entities,
                            too_large,
                            count,
                            fetch_failed,
                        ) = _fetch_platform_entities("local-authority:ABC", "tree")

        assert entities == []
        assert fetch_failed is True
        assert too_large is False
        assert count == 800

    def test_successful_fetch_reports_no_failure(self, app):
        with app.app_context():
            with patch(f"{TRANSFORM_MODULE}.get_org_entity", return_value=400):
                with patch(
                    f"{TRANSFORM_MODULE}.get_entity_count_for_organisation_and_dataset",
                    return_value=2,
                ):
                    with patch(
                        f"{TRANSFORM_MODULE}.get_entities_for_organisation_and_dataset",
                        return_value=[{"entity": 1}, {"entity": 2}],
                    ):
                        (
                            entities,
                            too_large,
                            count,
                            fetch_failed,
                        ) = _fetch_platform_entities("local-authority:ABC", "tree")

        assert len(entities) == 2
        assert fetch_failed is False


class TestFetchPlatformEntitiesCountUnavailable:
    def test_unavailable_count_suppresses_the_comparison(self, app):
        # Without a count the too-large guard can't be evaluated, and paging would
        # fall back to an unbounded serial walk inside the request.
        with app.app_context():
            with patch(f"{TRANSFORM_MODULE}.get_org_entity", return_value=400):
                with patch(
                    f"{TRANSFORM_MODULE}.get_entity_count_for_organisation_and_dataset",
                    return_value=None,
                ):
                    with patch(
                        f"{TRANSFORM_MODULE}.get_entities_for_organisation_and_dataset"
                    ) as fetch_entities:
                        (
                            entities,
                            too_large,
                            count,
                            fetch_failed,
                        ) = _fetch_platform_entities("local-authority:ABC", "tree")

        assert entities == []
        assert fetch_failed is True
        assert too_large is False
        assert count == 0
        fetch_entities.assert_not_called()


class TestGeometryFeaturesWithoutComparison:
    def _resource_details(self):
        return [
            {
                "entry_number": 1,
                "converted_row": {"reference": "R1", "name": "Area A"},
                "transformed_row": [
                    {"entity": 100, "field": "name", "value": "Area A"},
                    {"entity": 100, "field": "geometry", "value": "POINT (-2.5 54.5)"},
                ],
            }
        ]

    def test_compare_false_drops_status_and_platform_only_features(self):
        platform = [
            {"entity": 900, "reference": "P1", "geometry": "POINT (-1.0 53.0)"},
        ]
        features, points = _build_geometry_features(
            platform, self._resource_details(), "conservation-area", compare=False
        )

        # Only the resource's own geometry, and untagged so the map renders it as
        # one neutral group rather than miscategorising it.
        assert len(features) == 1
        assert features[0]["properties"]["entity"] == "100"
        assert "status" not in features[0]["properties"]
        assert all("status" not in p["properties"] for p in points)

    def test_compare_true_still_tags_and_includes_platform_entities(self):
        platform = [
            {"entity": 900, "reference": "P1", "geometry": "POINT (-1.0 53.0)"},
        ]
        features, _ = _build_geometry_features(
            platform, self._resource_details(), "conservation-area"
        )

        statuses = sorted(f["properties"]["status"] for f in features)
        assert statuses == ["existing", "new"]
