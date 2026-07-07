from application.blueprints.datamanager.controllers.preview import (
    _build_entity_organisation_summary,
)

NEW_ENTITIES = [{"entity": "10100002", "reference": "REF001"}]


def test_no_new_entities_hides_section():
    result = _build_entity_organisation_summary([], True, {"entity-organisation": []})

    assert result == (None, False, None, None, None)


def test_non_authoritative_is_informational_only():
    """Non-authoritative just flags the data as such - nothing needs to be created."""
    (
        table_params,
        has_entity_org,
        warning,
        overlap_warning,
        error_warning,
    ) = _build_entity_organisation_summary(NEW_ENTITIES, False, {"entity-organisation": []})

    assert table_params is None
    assert has_entity_org is False
    assert warning == "Non-authoritative data being submitted"
    assert overlap_warning is None
    assert error_warning is None


def test_authoritative_overlap_shows_table_and_overlap_warning():
    pipeline_summary = {
        "entity-organisation": [
            {
                "dataset": "nature-improvement-area",
                "entity-minimum": 10100002,
                "entity-maximum": 10100002,
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
        overlap_warning,
        error_warning,
    ) = _build_entity_organisation_summary(NEW_ENTITIES, True, pipeline_summary)

    assert has_entity_org is True
    assert table_params is not None
    assert warning is None
    assert overlap_warning == (
        "Entity org range mapping already assigned for these new "
        "entities - likely a single source dataset"
    )
    assert error_warning is None


def test_authoritative_error_shows_table_and_error_warning():
    pipeline_summary = {
        "entity-organisation": [
            {
                "dataset": "nature-improvement-area",
                "entity-minimum": 10100002,
                "entity-maximum": 10100002,
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
        overlap_warning,
        error_warning,
    ) = _build_entity_organisation_summary(NEW_ENTITIES, True, pipeline_summary)

    assert has_entity_org is True
    assert table_params is not None
    assert warning is None
    assert overlap_warning is None
    assert error_warning == (
        "An error occurred creating the entity-organisation csv, "
        "please re-run if you believe this is required"
    )


def test_authoritative_no_entity_organisation_data_hides_section():
    result = _build_entity_organisation_summary(
        NEW_ENTITIES, True, {"entity-organisation": []}
    )

    assert result == (None, False, None, None, None)
