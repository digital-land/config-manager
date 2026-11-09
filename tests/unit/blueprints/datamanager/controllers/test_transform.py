import json
from threading import Barrier, Event, get_ident

import pytest
from unittest.mock import patch

from application.utils import compute_hash
from application.blueprints.datamanager.controllers.transform import (
    _dedup_candidate_form_value,
    _build_geometry_features,
    _build_entities_data,
    _geometries_differ,
    _compare_entity_fields,
    ComparisonTimedOut,
    handle_check_transform,
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


def _geometry_detail(entity, geometry):
    return {
        "entry_number": entity,
        "transformed_row": [
            {"entity": entity, "field": "geometry", "value": geometry},
        ],
    }


def test_results_compare_once_and_reuse_all_categories_after_filtering(app):
    details = [
        _geometry_detail(100, "POINT (0 0)"),
        _geometry_detail(101, "POINT (1 1)"),
        _geometry_detail(102, "POINT (2 2)"),
    ]
    platform = [
        {"entity": 100, "geometry": "POINT (0 0)"},
        {"entity": 101, "geometry": "POINT (5 5)"},
        {"entity": 103, "geometry": "POINT (3 3)"},
    ]
    with app.test_request_context("/?entity_filter=new"), patch.multiple(
        TRANSFORM_MODULE,
        get_organisation_name=lambda code: "Test Org",
        get_dataset_name=lambda code, default: "Green belt",
        get_dataset_typology=lambda code: "geography",
        is_processing=lambda req: False,
        fetch_response_details=lambda request_id: (details, len(details)),
        _fetch_platform_entities=lambda org, dataset: (platform, False, 3, False),
        _resolve_existing_endpoints=lambda summary, url: [],
        fetch_boundary_geojson=lambda org: {},
        check_endpoint_in_doc=lambda doc, url: {},
        render_template=lambda name, **context: context,
    ), patch(
        f"{TRANSFORM_MODULE}._geometries_differ", wraps=_geometries_differ
    ) as compare:
        context = handle_check_transform(
            "reuse-test",
            {
                "params": {
                    "dataset": "green-belt",
                    "organisation": "local-authority:ABC",
                }
            },
        )

    # Only two shared entities need comparing; map construction must not repeat it.
    assert compare.call_count == 2
    assert [row["fields"]["entity"] for row in context["entities_data"]["rows"]] == [
        "102"
    ]
    assert context["category_counts"] == {
        "new": 1,
        "changed": 1,
        "in_both": 1,
        "existing": 1,
    }
    expected = {"100": "in_both", "101": "changed", "102": "new", "103": "existing"}
    for features in (context["geometries"], context["geometry_points"]):
        assert {
            feature["properties"]["entity"]: feature["properties"]["status"]
            for feature in features
        } == expected


def test_entity_geometry_comparisons_overlap_with_bounded_workers():
    # Every group of four has to enter before any can finish. Serial execution
    # would fail the barrier rather than pass due to a timing assumption.
    barrier = Barrier(4, timeout=5)
    worker_ids = []
    details = [_geometry_detail(entity, f"POINT ({entity} 0)") for entity in range(8)]
    platform = [
        {"entity": entity, "geometry": f"POINT ({entity} 0)"} for entity in range(8)
    ]

    def compare(resource, existing):
        worker_ids.append(get_ident())
        barrier.wait()
        return _geometries_differ(resource, existing)

    with patch(f"{TRANSFORM_MODULE}._geometries_differ", side_effect=compare):
        result = _build_entities_data(details, platform)

    assert len(set(worker_ids)) == 4
    assert get_ident() not in worker_ids
    assert [row["fields"]["entity"] for row in result["rows"]] == list(
        map(str, range(8))
    )
    assert all(row["category"] == "in_both" for row in result["rows"])


def test_parallel_comparison_failure_is_propagated():
    details = [_geometry_detail(entity, "POINT (0 0)") for entity in (100, 101)]
    platform = [{"entity": entity, "geometry": "POINT (0 0)"} for entity in (100, 101)]
    with patch(
        f"{TRANSFORM_MODULE}._geometries_differ",
        side_effect=RuntimeError("comparison failed"),
    ), pytest.raises(RuntimeError, match="comparison failed"):
        _build_entities_data(details, platform)


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("compare", [False, True])
def test_repeated_entity_geometries_keep_their_own_status(reverse, compare):
    geometries = ["POINT (0 0)", "POINT (1 1)"]
    statuses = ["in_both", "changed"]
    if reverse:
        geometries.reverse()
        statuses.reverse()
    # Normalised IDs must be treated as duplicates too.
    details = [
        _geometry_detail(100, geometries[0]),
        _geometry_detail("100.0", geometries[1]),
        _geometry_detail(101, "POINT (2 2)"),
    ]
    platform = [
        {"entity": 100, "geometry": "POINT (0 0)"},
        {"entity": 101, "geometry": "POINT (2 2)"},
    ]
    table = _build_entities_data(details, platform)
    categories = {row["fields"]["entity"]: row["category"] for row in table["rows"]}
    assert categories["100"] == statuses[-1]

    with patch(
        f"{TRANSFORM_MODULE}._geometries_differ", wraps=_geometries_differ
    ) as geometry_compare:
        features, points = _build_geometry_features(
            platform,
            details,
            "green-belt",
            compare=compare,
            entity_categories=categories,
        )

    for collection in (features, points):
        assert len(collection) == 3
        if compare:
            assert [feature["properties"]["status"] for feature in collection] == (
                statuses + ["in_both"]
            )
        else:
            assert all("status" not in feature["properties"] for feature in collection)
    # Unique entity 101 reuses its result; only the repeated rows need comparing.
    assert geometry_compare.call_count == (2 if compare else 0)


@pytest.mark.parametrize("entity_count", [1, 8])
def test_comparison_timeout_returns_before_running_work_finishes(
    monkeypatch, entity_count
):
    started, release, finished = Event(), Event(), Event()
    monkeypatch.setattr(f"{TRANSFORM_MODULE}._ENTITY_COMPARISON_TIMEOUT", 0.1)
    monkeypatch.setattr(f"{TRANSFORM_MODULE}._ENTITY_COMPARISON_WORKERS", 1)
    details = [
        _geometry_detail(entity, "POINT (0 0)") for entity in range(entity_count)
    ]
    platform = [
        {"entity": entity, "geometry": "POINT (0 0)"} for entity in range(entity_count)
    ]

    def slow_comparison(resource, existing):
        started.set()
        try:
            assert release.wait(
                5
            ), "Request waited for running comparison after timeout"
            return False
        finally:
            finished.set()

    with patch(
        f"{TRANSFORM_MODULE}._geometries_differ", side_effect=slow_comparison
    ) as comparison:
        try:
            with pytest.raises(ComparisonTimedOut):
                _build_entities_data(details, platform)
            assert started.is_set()
            assert not finished.is_set()
            assert comparison.call_count == 1
        finally:
            release.set()
            assert finished.wait(2)
    # The remaining entities must never start after the budget expires.
    assert comparison.call_count == 1


def test_expired_comparison_budget_does_not_start_work():
    with patch(f"{TRANSFORM_MODULE}._diff_entity_fields") as compare:
        with pytest.raises(ComparisonTimedOut):
            _compare_entity_fields([({}, {})], deadline=0)
    compare.assert_not_called()


@pytest.mark.parametrize("timeout_stage", ["table", "map"])
def test_comparison_timeout_discards_results_and_preserves_resource_data(
    app, timeout_stage
):
    from flask import render_template

    details = [
        _geometry_detail(100, "POINT (0 0)"),
        _geometry_detail("100.0", "POINT (1 1)"),
    ]
    details[0]["issue_logs"] = [
        {
            "issue-type": "test-issue",
            "message": "An example issue",
            "severity": "warning",
        }
    ]
    platform = [{"entity": 100, "geometry": "POINT (0 0)"}]
    deadlines = []

    def compare(pairs, deadline=None):
        if not pairs:
            return []
        deadlines.append(deadline)
        if timeout_stage == "table" or len(deadlines) == 2:
            raise ComparisonTimedOut()
        return _compare_entity_fields(pairs, deadline=deadline)

    with app.test_request_context("/"), patch.multiple(
        TRANSFORM_MODULE,
        get_organisation_name=lambda code: "Test Org",
        get_dataset_name=lambda code, default: "Green belt",
        get_dataset_typology=lambda code: "geography",
        is_processing=lambda req: False,
        fetch_response_details=lambda request_id: (details, len(details)),
        _fetch_platform_entities=lambda org, dataset: (platform, False, 1, False),
        _resolve_existing_endpoints=lambda summary, url: [],
        fetch_boundary_geojson=lambda org: {},
        check_endpoint_in_doc=lambda doc, url: {},
    ), patch(f"{TRANSFORM_MODULE}._compare_entity_fields", side_effect=compare), patch(
        f"{TRANSFORM_MODULE}.render_template", wraps=render_template
    ) as render:
        html = handle_check_transform(
            "timeout-test",
            {
                "params": {
                    "dataset": "green-belt",
                    "organisation": "local-authority:ABC",
                }
            },
        )

    context = render.call_args.kwargs
    assert context["comparison_timed_out"] is True
    assert context["comparison_unavailable"] is True
    assert context["entities_data"]["rows"] == []
    assert not any(context["category_counts"].values())
    assert len(context["transformed_table"]["rows"]) == 2
    assert len(context["issue_log_table"]["rows"]) == 1
    assert "Comparison took too long" in html
    assert "An example issue" in html
    assert "platform data could not be fetched" not in html
    assert "app-stat-box" not in html
    for features in (context["geometries"], context["geometry_points"]):
        assert len(features) == 2
        assert all("status" not in feature["properties"] for feature in features)
    if timeout_stage == "map":
        assert len(deadlines) == 2
        assert deadlines[0] == deadlines[1]


@pytest.mark.parametrize(
    "invalid",
    [
        None,
        "missing-row",
        "wrong-entity",
        "unknown-version",
        "unavailable",
        "too-large",
    ],
)
def test_prepared_comparison_never_fetches_or_compares_platform(app, invalid):
    details = [
        {**_geometry_detail(100, "POINT (0 0)"), "entry_number": 1},
        {**_geometry_detail(100, "POINT (1 1)"), "entry_number": 2},
        {**_geometry_detail(200, "POINT (2 2)"), "entry_number": 3},
    ]
    comparisons = [
        {
            "source": "resource",
            "entity": "100",
            "entry_number": "1",
            "category": "in_both",
            "changed_fields": {},
        },
        {
            "source": "resource",
            "entity": "100",
            "entry_number": "2",
            "category": "changed",
            "changed_fields": {"geometry": "(different geometry on platform)"},
        },
        {
            "source": "resource",
            "entity": "200",
            "entry_number": "3",
            "category": "new",
            "changed_fields": {},
        },
        {
            "source": "platform",
            "entity": "999",
            "category": "existing",
            "changed_fields": {},
            "fields": {
                "entity": 999,
                "name": "Platform only",
                "geometry": "POINT (9 9)",
            },
        },
    ]
    summary = {"version": 1, "status": "complete", "row_count": 4, "existing_count": 2}
    if invalid == "missing-row":
        comparisons.pop(1)
    elif invalid == "wrong-entity":
        comparisons[0]["entity"] = "444"
    elif invalid == "unknown-version":
        summary["version"] = 2
    elif invalid in ("unavailable", "too-large"):
        summary["status"] = invalid

    with app.test_request_context("/?entity_filter=new"), patch.multiple(
        TRANSFORM_MODULE,
        get_organisation_name=lambda code: "Test Org",
        get_dataset_name=lambda code, default: "Green belt",
        get_dataset_typology=lambda code: "geography",
        is_processing=lambda req: False,
        fetch_response_details=lambda request_id: (details, len(details)),
        fetch_platform_comparison=lambda request_id, expected: comparisons,
        _resolve_existing_endpoints=lambda summary, url: [],
        fetch_boundary_geojson=lambda org: {},
        check_endpoint_in_doc=lambda doc, url: {},
        render_template=lambda name, **context: context,
    ), patch(
        f"{TRANSFORM_MODULE}._fetch_platform_entities",
        side_effect=AssertionError("Must not fetch platform"),
    ), patch(
        f"{TRANSFORM_MODULE}._diff_entity_fields",
        side_effect=AssertionError("Must not compare in Manage"),
    ), patch(
        f"{TRANSFORM_MODULE}._build_entities_data",
        side_effect=AssertionError("Must use prepared table"),
    ):
        context = handle_check_transform(
            "prepared-test",
            {
                "params": {
                    "dataset": "green-belt",
                    "organisation": "local-authority:ABC",
                },
                "response": {
                    "data": {"pipeline-summary": {"platform-comparison": summary}}
                },
            },
        )
    if invalid is None:
        assert context["comparison_unavailable"] is False
        assert context["category_counts"] == {
            "new": 1,
            "changed": 1,
            "in_both": 0,
            "existing": 1,
        }
        assert [
            row["fields"]["entity"] for row in context["entities_data"]["rows"]
        ] == ["200"]
        for features in (context["geometries"], context["geometry_points"]):
            assert [feature["properties"]["status"] for feature in features] == [
                "existing",
                "in_both",
                "changed",
                "new",
            ]
    else:
        assert context["comparison_unavailable"] or context["platform_too_large"]
        assert context["entities_data"]["rows"] == []
        assert all(
            "status" not in feature["properties"] for feature in context["geometries"]
        )
