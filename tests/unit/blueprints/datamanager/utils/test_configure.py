from application.blueprints.datamanager.utils.configure import build_column_mapping_rows


def test_build_column_mapping_rows_orders_mapped_then_unmapped_by_field():
    rows = build_column_mapping_rows(
        column_field_log=[
            {"column": "user_name", "field": "name"},
            {"column": "user_ref", "field": "reference"},
        ],
        unmapped_columns={"z_col", "a_col"},
        spec_fields={"geometry", "entry-date", "name", "reference"},
    )

    assert [row["field"] for row in rows] == [
        "name",
        "reference",
        "entry-date",
        "geometry",
    ]


def test_build_column_mapping_rows_sorts_dropdown_columns_alphabetically():
    rows = build_column_mapping_rows(
        column_field_log=[],
        unmapped_columns={"z_col", "a_col", "middle_col"},
        spec_fields={"geometry"},
    )

    assert rows[0]["available_columns"] == ["a_col", "middle_col", "z_col"]
