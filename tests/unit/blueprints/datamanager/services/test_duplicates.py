from application.blueprints.datamanager.services.duplicates import (
    _normalise_entity_id,
    parse_selected_redirects,
)


def test_normalise_entity_id_keeps_fractional_and_non_numeric_values():
    assert _normalise_entity_id("100") == "100"
    assert _normalise_entity_id("100.0") == "100"
    assert _normalise_entity_id("100.5") == "100.5"
    assert _normalise_entity_id("abc") == "abc"
    assert _normalise_entity_id(None) == ""
    assert _normalise_entity_id("") == ""


def test_parse_selected_redirects_filters_invalid_rows():
    redirects = parse_selected_redirects(
        [
            '{"old_entity":"100","entity":"200","dataset":"tree","match_type":"complete_match"}',
            '{"old_entity":"","entity":"201","dataset":"tree"}',
            "not-json",
        ],
        [{"old_entity": "100", "entity": "200", "dataset": "tree"}],
    )

    assert redirects == [
        {
            "old_entity": "100",
            "entity": "200",
            "dataset": "tree",
            "old_reference": "",
            "new_reference": "",
            "match_type": "complete_match",
            "notes": "Redirect duplicate entity selected in Assign Entities",
        }
    ]


def test_parse_selected_redirects_validates_against_duplicate_candidates():
    redirects = parse_selected_redirects(
        [
            '{"old_entity":"100","entity":"200","dataset":"tree"}',
            '{"old_entity":"999","entity":"200","dataset":"tree"}',
        ],
        duplicate_candidates=[
            {"old_entity": "100", "entity": "200", "dataset": "tree"}
        ],
    )

    assert [redirect["old_entity"] for redirect in redirects] == ["100"]


def test_parse_selected_redirects_skips_repeated_old_entities():
    redirects = parse_selected_redirects(
        [
            '{"old_entity":"100","entity":"200","dataset":"tree"}',
            '{"old_entity":"100","entity":"201","dataset":"tree"}',
        ],
        duplicate_candidates=[
            {"old_entity": "100", "entity": "200", "dataset": "tree"},
            {"old_entity": "100", "entity": "201", "dataset": "tree"},
        ],
    )

    assert redirects == [
        {
            "old_entity": "100",
            "entity": "200",
            "dataset": "tree",
            "old_reference": "",
            "new_reference": "",
            "match_type": "",
            "notes": "Redirect duplicate entity selected in Assign Entities",
        }
    ]
