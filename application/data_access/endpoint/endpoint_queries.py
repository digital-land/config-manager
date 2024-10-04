from application.data_access.datasette_utils import get_datasette_query


def get_endpoint_details(endpoint_hash, pipeline):
    logs_df = get_logs(endpoint_hash)
    logs_headers = list(
        map(
            lambda x: {"text": x, "classes": "reporting-table-header"},
            logs_df.columns.values.tolist(),
        )
    )
    logs_rows = []
    for row in logs_df.values.tolist():
        logs_rows.append(
            list(map(lambda x: {"text": x, "classes": "reporting-table-cell"}, row))
        )

    resources_df = get_resources(endpoint_hash)
    resources_headers = list(
        map(
            lambda x: {"text": x, "classes": "reporting-table-header"},
            resources_df.columns.values.tolist(),
        )
    )
    resources_rows = []
    for row in resources_df.values.tolist():
        resources_rows.append(
            list(map(lambda x: {"text": x, "classes": "reporting-table-cell"}, row))
        )

    endpoint_info_df = get_endpoint_info(endpoint_hash, pipeline)
    endpoint_info = endpoint_info_df.to_dict(orient="records")
    return {
        "logs_headers": logs_headers,
        "logs_rows": logs_rows,
        "resources_headers": resources_headers,
        "resources_rows": resources_rows,
        "endpoint_info": endpoint_info[0],
    }


def get_logs(endpoint_hash):
    sql = f"""
    select
        substring(entry_date,1,10) as entry_date,
        case
            when (status = '') then exception
            else status
        end as status,
        resource
    from
        log
    where
        endpoint = '{endpoint_hash}'
    order by
        entry_date desc
    """
    return get_datasette_query("digital-land", sql)


def get_resources(endpoint_hash):
    sql = f"""
    select
        r.resource,
        r.start_date,
        r.end_date
    from
        resource r
        inner join resource_endpoint re on re.resource = r.resource
    where
        re.endpoint = '{endpoint_hash}'
    order by
        r.start_date desc
    """
    return get_datasette_query("digital-land", sql)


def get_endpoint_info(endpoint_hash, dataset):
    sql = f"""
    select
        sp.pipeline,
        o.name as organisation_name,
        s.organisation,
        e.endpoint,
        e.endpoint_url,
        e.start_date,
        substring(e.entry_date,1,10) as entry_date
    from
        endpoint e
        inner join source s on s.endpoint = e.endpoint
        inner join source_pipeline sp on sp.source = s.source
        inner join organisation o on o.organisation = replace(s.organisation, '-eng', '')
    where
        s.endpoint = '{endpoint_hash}'
        AND sp.pipeline = '{dataset}'
    """
    return get_datasette_query("digital-land", sql)
