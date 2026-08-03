import json

from application.blueprints.datamanager.controllers.transform import (
    _dedup_candidate_form_value,
    _dedup_dynamic_columns,
    _prepare_duplicate_candidates,
    _show_dedup_tab,
)


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
