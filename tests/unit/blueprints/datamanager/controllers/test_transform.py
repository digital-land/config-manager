import json

from application.blueprints.datamanager.controllers.transform import (
    _dedup_candidate_form_value,
    _prepare_duplicate_candidates,
)
from application.blueprints.datamanager.services.duplicates import REDIRECT_NOTE


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
        "entity": "200",
        "dataset": "conservation-area",
        "old_reference": "old-ref",
        "new_reference": "new-ref",
        "match_type": "complete_match",
        "notes": REDIRECT_NOTE,
    }


def test_dedup_candidate_form_value_keeps_existing_value():
    assert _dedup_candidate_form_value({"form_value": '{"entity":"200"}'}) == (
        '{"entity":"200"}'
    )


def test_prepare_duplicate_candidates_auto_selects_complete_matches():
    candidates = _prepare_duplicate_candidates(
        [
            {
                "match_type": "complete_match",
                "name_similarity": 10,
            }
        ]
    )

    assert candidates[0]["auto_select"] is True


def test_prepare_duplicate_candidates_auto_selects_single_matches_over_threshold():
    candidates = _prepare_duplicate_candidates(
        [
            {
                "match_type": "single_match",
                "name_similarity": 86,
            }
        ]
    )

    assert candidates[0]["auto_select"] is True


def test_prepare_duplicate_candidates_does_not_auto_select_single_matches_at_threshold():
    candidates = _prepare_duplicate_candidates(
        [
            {
                "match_type": "single_match",
                "name_similarity": 85,
            }
        ]
    )

    assert candidates[0]["auto_select"] is False
