from application.blueprints.datamanager.controllers.preview import (
    _build_entity_organisation_summary,
    build_old_entity_redirect_table,
)

NEW_ENTITIES = [{"entity": "10100002", "reference": "REF001"}]


def test_old_entity_redirect_table_renders_null_values_as_empty_strings():
    table_params = build_old_entity_redirect_table(
        [
            {
                "old-entity": None,
                "status": None,
                "entity": None,
                "notes": None,
                "end-date": None,
                "entry-date": None,
                "start-date": None,
            }
        ]
    )

    row = table_params["rows"][0]["columns"]
    assert row["old-entity"]["value"] == ""
    assert row["status"]["value"] == ""
    assert row["entity"]["value"] == ""
    assert row["notes"]["value"] == ""
    assert row["end-date"]["value"] == ""
    assert row["entry-date"]["value"] == ""
    assert row["start-date"]["value"] == ""


def test_old_entity_redirect_table_hides_target_for_retirement():
    table_params = build_old_entity_redirect_table(
        [
            {
                "old-entity": "100",
                "status": "410",
                "entity": "200",
            }
        ]
    )

    row = table_params["rows"][0]["columns"]
    assert row["old-entity"]["value"] == "100"
    assert row["status"]["value"] == "410"
    assert row["entity"]["value"] == ""


def test_no_new_entities_hides_section():
    result = _build_entity_organisation_summary([], True, {"entity-organisation": []})

    assert result == (None, False, None, None, None)


def test_non_authoritative_is_informational_only():
    """Non-authoritative just flags the data as such - nothing needs to be created."""
    (
        table_params,
        has_entity_org,
        warning,
        overlap_info,
        error_warning,
    ) = _build_entity_organisation_summary(
        NEW_ENTITIES, False, {"entity-organisation": []}
    )

    assert table_params is None
    assert has_entity_org is False
    assert warning == "Non-authoritative data being submitted"
    assert overlap_info is None
    assert error_warning is None


def test_authoritative_overlap_shows_info_message_only():
    """Overlap is informational, not a warning, and the table is skipped."""
    pipeline_summary = {
        "entity-organisation": [
            {
                "dataset": "nature-improvement-area",
                "organisation": "government-organisation:PB202",
                "overlap": True,
                "error": False,
            }
        ]
    }

    (
        table_params,
        has_entity_org,
        warning,
        overlap_info,
        error_warning,
    ) = _build_entity_organisation_summary(NEW_ENTITIES, True, pipeline_summary)

    assert has_entity_org is False
    assert table_params is None
    assert warning is None
    assert overlap_info == "Entity org already exists - no action needed"
    assert error_warning is None


def test_authoritative_error_shows_error_message_only():
    """Error skips the table too, since there's no trustworthy range to show."""
    pipeline_summary = {
        "entity-organisation": [
            {
                "dataset": "nature-improvement-area",
                "organisation": "government-organisation:PB202",
                "overlap": False,
                "error": True,
            }
        ]
    }

    (
        table_params,
        has_entity_org,
        warning,
        overlap_info,
        error_warning,
    ) = _build_entity_organisation_summary(NEW_ENTITIES, True, pipeline_summary)

    assert has_entity_org is False
    assert table_params is None
    assert warning is None
    assert overlap_info is None
    assert error_warning == (
        "An error occurred creating the entity-organisation csv, "
        "please re-run if you believe this is required"
    )


def test_authoritative_no_overlap_or_error_shows_table():
    pipeline_summary = {
        "entity-organisation": [
            {
                "dataset": "nature-improvement-area",
                "entity-minimum": 10100002,
                "entity-maximum": 10100002,
                "organisation": "government-organisation:PB202",
                "overlap": False,
                "error": False,
            }
        ]
    }

    (
        table_params,
        has_entity_org,
        warning,
        overlap_info,
        error_warning,
    ) = _build_entity_organisation_summary(NEW_ENTITIES, True, pipeline_summary)

    assert has_entity_org is True
    assert table_params is not None
    assert warning is None
    assert overlap_info is None
    assert error_warning is None


def test_authoritative_no_entity_organisation_data_hides_section():
    result = _build_entity_organisation_summary(
        NEW_ENTITIES, True, {"entity-organisation": []}
    )

    assert result == (None, False, None, None, None)
