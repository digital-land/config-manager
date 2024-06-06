from application.data_access.datasette_utils import get_datasette_query
from application.data_access.overview.api_queries import get_organisation_entity_number
from application.utils import split_organisation_id


def get_grouped_entity_count(dataset=None, organisation_entity=None):
    query_lines = [
        "SELECT COUNT(dataset) AS count,",
        "dataset",
        "FROM",
        "entity",
    ]
    if organisation_entity:
        query_lines.append("WHERE")
        query_lines.append(f"organisation_entity = '{organisation_entity}'")
    if dataset:
        if "WHERE" not in query_lines:
            query_lines.append("WHERE")
        else:
            query_lines.append("AND")
        query_lines.append(f"dataset = '{dataset}'")
    else:
        query_lines.append("GROUP BY")
        query_lines.append("dataset")

    query_str = " ".join(query_lines)

    rows = get_datasette_query("entity", f"""{query_str}""")
    if rows is not None and not rows.empty:
        return {row["dataset"]: row["count"] for index, row in rows.iterrows()}

    return {}


def get_total_entity_count():
    sql = "select count(*) from (select * from entity)"
    row = get_datasette_query("entity", sql)

    return row.iloc[0][0] if len(row) > 0 else 0


def get_entity_count(pipeline=None):
    if pipeline is not None:
        sql = "SELECT COUNT(*) FROM (SELECT * FROM entity WHERE dataset = :pipeline)"
        row = get_datasette_query("entity", sql, {"pipeline": pipeline})
        return row.iloc[0][0] if len(row) > 0 else 0
    return get_total_entity_count()


def get_organisation_entities_using_end_dates():
    sql = """
        SELECT organisation_entity
            FROM entity WHERE end_date != ""
            AND ("organisation_entity" is not null and "organisation_entity" != "")
            GROUP BY organisation_entity
        """

    return get_datasette_query("entity", f"{sql}")


def get_organisation_entity_count(organisation, dataset=None):
    prefix, ref = split_organisation_id(organisation)
    return get_grouped_entity_count(
        dataset=dataset,
        organisation_entity=get_organisation_entity_number(prefix, ref),
    )


def get_datasets_organisation_has_used_enddates(organisation):
    prefix, ref = split_organisation_id(organisation)
    organisation_entity_num = get_organisation_entity_number(prefix, ref)
    if not organisation_entity_num:
        return None
    query_lines = [
        "SELECT",
        "dataset",
        "FROM",
        "entity_end_date_counts",
        "WHERE",
        '("end_date" is not null and "end_date" != "")',
        "AND",
        f'("organisation_entity" = {organisation_entity_num})',
        "GROUP BY",
        "dataset",
    ]
    query_str = " ".join(query_lines)
    rows = get_datasette_query("entity", query_str)
    if len(rows) > 0:
        return [dataset[0] for dataset in rows]
    return []
    # columns = rows.columns.tolist()
    # with SqliteDatabase(entity_stats_db_path) as db:
    #     rows = db.execute(query_str).fetchall()
    # if rows:
    #     return [dataset[0] for dataset in rows]

    # return []
