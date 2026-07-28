import json
from datetime import date
from unittest.mock import patch

from application.utils import compute_hash
from application.blueprints.datamanager.controllers.transform import (
    _days_since,
    _dedup_candidate_form_value,
    _prepare_duplicate_candidates,
    _resolve_existing_endpoints,
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
        "entity": "200",
        "dataset": "conservation-area",
        "old_reference": "old-ref",
        "new_reference": "new-ref",
        "match_type": "complete_match",
        "notes": "Redirect duplicate entity selected in Assign Entities",
    }


def test_dedup_candidate_form_value_keeps_existing_value():
    assert _dedup_candidate_form_value({"form_value": '{"entity":"200"}'}) == (
        '{"entity":"200"}'
    )


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
        organisation="local-authority:ABC",
        selected_redirects=[
            {
                "reference": "ref-1",
                "old_entity_number": "100",
            }
        ],
    )

    assert candidates[0]["auto_select"] is True
    assert candidates[0]["redirect_locked"] is False


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
        organisation="local-authority:ABC",
        excluded_references=["ref-1"],
    )

    assert candidates[0]["redirect_can_select"] is False
    assert candidates[1]["redirect_can_select"] is True


def test_days_since_computes_whole_days():
    fixed = (date.today() - date(2020, 1, 1)).days
    assert _days_since("2020-01-01T00:00:00Z") == fixed
    assert _days_since("2020-01-01") == fixed


def test_days_since_returns_none_for_empty_or_invalid():
    assert _days_since("") is None
    assert _days_since(None) is None
    assert _days_since("not-a-date") is None


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
            "latest_200_date": "2026-07-20",
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
    assert by_hash["hash-new"]["days-since-200"] is not None
    assert by_hash["hash-old"]["days-since-200"] is None
