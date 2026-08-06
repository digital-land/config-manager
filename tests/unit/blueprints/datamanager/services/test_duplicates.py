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
            '{"old_entity":"100","dataset":"tree","new_reference":"new-ref","match_type":"complete_match"}',
            '{"old_entity":"","entity":"201","dataset":"tree"}',
            "not-json",
        ],
        [
            {
                "old_entity": "100",
                "entity": "200",
                "dataset": "tree",
                "new_reference": "new-ref",
            }
        ],
    )

    assert redirects == [
        {
            "old_entity_number": "100",
            "reference": "new-ref",
            "status": "301",
        }
    ]


def test_parse_selected_redirects_validates_against_duplicate_candidates():
    redirects = parse_selected_redirects(
        [
            '{"old_entity":"100","dataset":"tree","new_reference":"new-ref"}',
            '{"old_entity":"999","dataset":"tree","new_reference":"new-ref"}',
        ],
        duplicate_candidates=[
            {
                "old_entity": "100",
                "entity": "200",
                "dataset": "tree",
                "new_reference": "new-ref",
            }
        ],
    )

    assert [redirect["old_entity_number"] for redirect in redirects] == ["100"]


def test_parse_selected_redirects_skips_repeated_old_entities():
    redirects = parse_selected_redirects(
        [
            '{"old_entity":"100","dataset":"tree","new_reference":"ref-1"}',
            '{"old_entity":"100","dataset":"tree","new_reference":"ref-2"}',
        ],
        duplicate_candidates=[
            {
                "old_entity": "100",
                "entity": "200",
                "dataset": "tree",
                "new_reference": "ref-1",
            },
            {
                "old_entity": "100",
                "entity": "201",
                "dataset": "tree",
                "new_reference": "ref-2",
            },
        ],
    )

    assert redirects == [
        {
            "old_entity_number": "100",
            "reference": "ref-1",
            "status": "301",
        }
    ]


def test_parse_selected_redirects_builds_retirement_without_target():
    redirects = parse_selected_redirects(
        ['{"old_entity":"100","dataset":"tree","status":"410"}'],
        [{"old_entity": "100", "entity": "200", "dataset": "tree"}],
    )

    assert redirects == [{"old_entity_number": "100", "status": "410"}]


def test_parse_selected_redirects_coerces_unsupported_status_to_301():
    redirects = parse_selected_redirects(
        [
            '{"old_entity":"100","dataset":"tree","new_reference":"new-ref","status":"302"}'
        ],
        [
            {
                "old_entity": "100",
                "entity": "200",
                "dataset": "tree",
                "new_reference": "new-ref",
            }
        ],
    )

    assert redirects == [
        {
            "old_entity_number": "100",
            "reference": "new-ref",
            "status": "301",
        }
    ]


def test_parse_selected_redirects_drops_excluded_redirects_but_keeps_retirements():
    redirects = parse_selected_redirects(
        [
            '{"old_entity":"100","dataset":"tree","new_reference":"excluded","status":"301"}',
            '{"old_entity":"100","dataset":"tree","new_reference":"included","status":"301"}',
            '{"old_entity":"101","dataset":"tree","status":"410"}',
        ],
        [
            {
                "old_entity": "100",
                "dataset": "tree",
                "new_reference": "excluded",
            },
            {
                "old_entity": "100",
                "dataset": "tree",
                "new_reference": "included",
            },
            {"old_entity": "101", "dataset": "tree"},
        ],
        excluded_references=["excluded"],
    )

    assert redirects == [
        {
            "reference": "included",
            "old_entity_number": "100",
            "status": "301",
        },
        {"old_entity_number": "101", "status": "410"},
    ]
