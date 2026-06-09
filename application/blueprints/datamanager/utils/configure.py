def build_column_mapping_rows(
    column_field_log, unmapped_columns, user_column_mapping=None, spec_fields=None
):
    """Build rows for the inline column mapping UI.

    Returns a list of dicts, each with:
      - field: the system field
      - column: the mapped CSV column name (or empty string for unmapped)
      - is_mapped: True if the field already has a column mapping
      - user_defined: True if the mapping was set by the user (i.e. in user_column_mapping)

    Rows are keyed by system field so users choose a source column for each field.
    """
    user_column_mapping = user_column_mapping or {}
    field_names = set(spec_fields or [])

    # The async service currently returns IGNORE for fields that have been unmapped.
    # Exclude these to avoid showing IGNORE as an unmapped field in the UI.
    field_names.update(
        entry.get("field")
        for entry in column_field_log
        if entry.get("field") and entry.get("field") != "IGNORE"
    )

    active_mapping = {}
    ignored_columns = set()
    ignored_fields = set()

    for entry in column_field_log:
        col = entry.get("column")
        field = entry.get("field", "")
        if not col or not field:
            continue
        if user_column_mapping.get(col) == "IGNORE":
            ignored_columns.add(col)
            ignored_fields.add(field)
            continue
        if field == "IGNORE":
            continue
        active_mapping[field] = col

    for col, field in user_column_mapping.items():
        if field == "IGNORE":
            ignored_columns.add(col)
            continue
        if field:
            field_names.add(field)
            active_mapping[field] = col

    mapped_columns = set(active_mapping.values())
    available_columns = sorted(
        (set(unmapped_columns) | ignored_columns) - mapped_columns
    )

    mapped_fields = sorted(field for field in field_names if active_mapping.get(field))
    unmapped_fields = sorted(
        field for field in field_names if not active_mapping.get(field)
    )

    rows = []
    for field in mapped_fields + unmapped_fields:
        column = active_mapping.get(field, "")
        rows.append(
            {
                "field": field,
                "column": column,
                "is_mapped": bool(column),
                "user_defined": bool(column)
                and user_column_mapping.get(column) == field,
                "user_ignored": field in ignored_fields,
                "available_columns": available_columns,
            }
        )

    return rows
