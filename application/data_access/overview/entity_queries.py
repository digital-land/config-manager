import json
import urllib.request

from application.data_access.datasette_utils import get_datasette_query
from application.data_access.overview.api_queries import get_organisation_entity_number
from application.utils import split_organisation_id


def get_grouped_entity_count(dataset=None, organisation_entity=None):
    query_lines = [
        "select count(prefix)as count,",
        "prefix",
        "FROM",
        "lookup",
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
        query_lines.append("prefix")

    query_str = " ".join(query_lines)

    rows = get_datasette_query("digital-land", f"""{query_str}""")
    if rows is not None and not rows.empty:
        return {row["prefix"]: row["count"] for index, row in rows.iterrows()}

    return {}


def get_entity_count(pipeline=None):
    url = f"https://www.planning.data.gov.uk/entity.json{f'?prefix={pipeline}' if pipeline else ''}"
    with urllib.request.urlopen(url) as response:
        data = json.load(response)
        return data["count"]


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
