from application.blueprints.datamanager.controllers.transform import (
    _prepare_duplicate_candidates,
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
