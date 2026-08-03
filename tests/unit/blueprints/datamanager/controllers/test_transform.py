import json
from unittest.mock import patch

from application.utils import compute_hash
from application.blueprints.datamanager.controllers.transform import (
    _dedup_candidate_form_value,
    _dedup_dynamic_columns,
    _prepare_duplicate_candidates,
    _resolve_existing_endpoints,
    _show_dedup_tab,
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


def test_show_dedup_tab_uses_typology_but_keeps_conservation_area_spatial_flow():
    assert _show_dedup_tab(True, "tree-preservation-order", "legal-instrument")
    assert _show_dedup_tab(True, "conservation-area", "geography")
    assert not _show_dedup_tab(True, "article-4-direction-area", "geography")
    assert not _show_dedup_tab(False, "tree-preservation-order", "legal-instrument")
