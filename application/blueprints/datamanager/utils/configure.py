def build_column_mapping_rows(column_field_log, unmapped_columns, user_column_mapping=None):
    """Build rows for the inline column mapping UI.

    Returns a list of dicts, each with:
      - column: the CSV column name
      - field: the mapped spec field (or empty string for unmapped)
      - is_mapped: True if the column already has a mapping from the pipeline
      - user_defined: True if the mapping was set by the user (i.e. in user_column_mapping)

    Mapped rows come first (from column_field_log), then unmapped rows
    (discovered in the converted data but absent from the log).
    """
    user_column_mapping = user_column_mapping or {}
    rows = []
    for entry in column_field_log:
        col = entry.get("column")
        field = entry.get("field", "")
        if col:
            user_ignored = user_column_mapping.get(col) == "IGNORE"
            rows.append(
                {
                    "column": col,
                    "field": field,
                    "is_mapped": not user_ignored,
                    "user_defined": col in user_column_mapping and not user_ignored,
                    "user_ignored": user_ignored,
                }
            )

    for col in sorted(unmapped_columns):
        rows.append(
            {
                "column": col,
                "field": "",
                "is_mapped": False,
                "user_defined": False,
                "user_ignored": False,
            }
        )

    return rows
