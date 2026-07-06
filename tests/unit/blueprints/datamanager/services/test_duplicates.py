from application.blueprints.datamanager.services.duplicates import (
    parse_selected_redirects,
)


def test_parse_selected_redirects_filters_invalid_rows():
    redirects = parse_selected_redirects(
        [
            '{"old_entity":"100","entity":"200","dataset":"tree","match_type":"complete_match"}',
            '{"old_entity":"","entity":"201","dataset":"tree"}',
            "not-json",
        ]
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
