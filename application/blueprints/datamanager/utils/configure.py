def build_column_mapping_rows(column_field_log, unmapped_columns):
    """Build rows for the inline column mapping UI.

    Returns a list of dicts, each with:
      - column: the CSV column name
      - field: the mapped spec field (or empty string for unmapped)
      - is_mapped: True if the column already has a mapping from the pipeline

    Mapped rows come first (from column_field_log), then unmapped rows
    (discovered in the converted data but absent from the log).
    """
    rows = []
    for entry in column_field_log:
        col = entry.get("column")
        field = entry.get("field", "")
        if col:
            rows.append({
                "column": col,
                "field": field,
                "is_mapped": True,
            })

    for col in sorted(unmapped_columns):
        rows.append({
            "column": col,
            "field": "",
            "is_mapped": False,
        })

    return rows
