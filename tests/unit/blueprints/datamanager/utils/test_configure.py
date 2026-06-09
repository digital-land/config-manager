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


def test_build_column_mapping_rows_flags_user_ignored_fields():
    rows = build_column_mapping_rows(
        column_field_log=[
            {"column": "geom", "field": "geometry"},
            {"column": "ref", "field": "reference"},
        ],
        unmapped_columns=set(),
        user_column_mapping={"geom": "IGNORE"},
        spec_fields={"geometry", "reference"},
    )

    geometry_row = next(row for row in rows if row["field"] == "geometry")

    assert geometry_row["user_ignored"] is True
    assert geometry_row["is_mapped"] is False
    assert geometry_row["available_columns"] == ["geom"]
