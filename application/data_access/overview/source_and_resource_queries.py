import datetime
import re

import pandas as pd

from application.data_access.datasette_utils import get_datasette_query
from application.data_access.overview.digital_land_queries import (
    get_datasets,
    get_monthly_source_counts,
)
from application.utils import index_by, month_dict, months_since, this_month, yesterday


# returns organisation counts per dataset
def publisher_coverage():
    # TODO handle when pipeline is not None
    sql = """
            SELECT
              source_pipeline.pipeline,
              count(DISTINCT source.organisation),
              count(DISTINCT provision.organisation),
              CASE
              WHEN COUNT(DISTINCT provision.organisation) = 0 THEN COUNT(DISTINCT source.organisation)
              ELSE COUNT(DISTINCT provision.organisation)
              END AS expected_publishers,
              COUNT(
                DISTINCT CASE
                  WHEN source.endpoint != ''
                  and source.organisation!='government-organisation:D1342'
                  THEN source.organisation
                END
              ) AS publishers
            FROM
              source
              INNER JOIN source_pipeline ON source.source = source_pipeline.source
              LEFT JOIN provision on source_pipeline.pipeline=provision.dataset
              and
              provision.end_date==""
            GROUP BY
            source_pipeline.pipeline
    """

    rows = get_datasette_query("digital-land", f"{sql}")
    columns = rows.columns.tolist()

    return [dict(zip(columns, row)) for row in rows.to_numpy()]


def resources_by_dataset():
    # TODO handle when pipeline is not None
    sql = """
        SELECT
      count(DISTINCT resource.resource) AS total_resources,
      count(
        DISTINCT CASE
          WHEN resource.end_date == '' THEN resource.resource
          WHEN strftime('%Y%m%d', resource.end_date) >= strftime('%Y%m%d', 'now') THEN resource.resource
        END
      ) AS active_resources,
      count(
        DISTINCT CASE
          WHEN resource.end_date != ''
          AND strftime('%Y%m%d', resource.end_date) <= strftime('%Y%m%d', 'now') THEN resource.resource
        END
      ) AS ended_resources,
      source_pipeline.pipeline
    FROM
      resource
      INNER JOIN resource_endpoint ON resource.resource = resource_endpoint.resource
      INNER JOIN source ON source.endpoint = resource_endpoint.endpoint
      INNER JOIN source_pipeline ON source.source = source_pipeline.source
    GROUP BY
      source_pipeline.pipeline
    """
    rows = get_datasette_query("digital-land", f"{sql}")
    columns = rows.columns.tolist()

    return [dict(zip(columns, row)) for row in rows.to_numpy()]


def first_and_last_resource():
    # used by get_datasets_summary
    sql = """
          SELECT
          resource.resource,
          MAX(resource.start_date) AS latest,
          MIN(resource.start_date) AS first,
          source_pipeline.pipeline
        FROM
          resource
          INNER JOIN resource_endpoint ON resource.resource = resource_endpoint.resource
          INNER JOIN source ON resource_endpoint.endpoint = source.endpoint
          INNER JOIN source_pipeline ON source.source = source_pipeline.source
        GROUP BY
          source_pipeline.pipeline
        ORDER BY
          resource.start_date DESC"""

    rows = get_datasette_query("digital-land", sql)
    columns = rows.columns.tolist()

    return [dict(zip(columns, row)) for row in rows.to_numpy()]


def latest_endpoint_entry_date():
    # used by get_datasets_summary
    sql = """
          select pipeline,substr(MAX(endpoint_entry_date), 1,
          instr(MAX(endpoint_entry_date), 'T') - 1) as latest_endpoint
          from reporting_latest_endpoints group by pipeline"""

    rows = get_datasette_query("digital-land", sql)
    columns = rows.columns.tolist()

    return [dict(zip(columns, row)) for row in rows.to_numpy()]


def active_and_total_endpoints():
    # used by get_datasets_summary
    sql = """
          SELECT
          pipeline,
          COUNT(DISTINCT endpoint) AS total,
          COUNT(CASE WHEN status == '200' THEN status END) as active
          FROM (
            SELECT
                pipeline,
                endpoint,
                status
            FROM
                reporting_historic_endpoints
            WHERE
                endpoint_end_date = ""
            GROUP BY
                pipeline, endpoint
        ) AS subquery
          GROUP BY
          pipeline;"""

    rows = get_datasette_query("digital-land", sql)
    columns = rows.columns.tolist()

    return [dict(zip(columns, row)) for row in rows.to_numpy()]


def get_typology():
    # used by get_datasets_summary
    sql = """
          select dataset as pipeline, typology from dataset group by dataset"""

    rows = get_datasette_query("digital-land", sql)
    columns = rows.columns.tolist()

    return [dict(zip(columns, row)) for row in rows.to_numpy()]


def get_spec_filename(dataset_urls):
    if pd.isna(dataset_urls) or not isinstance(dataset_urls, str):
        return []

    urls = dataset_urls.split(";")

    # extract dataset name from specification file URL
    filenames = [
        re.search(r"/([^/]+)\.md", url).group(1)
        for url in urls
        if re.search(r"/([^/]+)\.md", url)
    ]

    return filenames


def get_frequency():
    # used by get_datasets_summary
    df = pd.read_csv(
        "https://design.planning.data.gov.uk/planning-consideration/planning-considerations.csv"
    )

    if "datasets" not in df.columns or "frequency-of-updates" not in df.columns:
        raise ValueError(
            "CSV must contain 'datasets' and 'frequency-of-updates' columns"
        )

    df["frequency-of-updates"].fillna("", inplace=True)
    df["pipeline"] = df["datasets"].apply(get_spec_filename)
    df = df.explode("pipeline")
    columns = ["pipeline", "frequency-of-updates"]
    data = df[columns].to_dict(orient="records")

    return data


def get_datasets_summary():
    # get all the datasets listed with their active status
    all_datasets = index_by("dataset", get_datasets())

    missing = []

    # add the publisher coverage numbers
    dataset_coverage = publisher_coverage()
    for d in dataset_coverage:
        if all_datasets.get(d["pipeline"]):
            all_datasets[d["pipeline"]] = {**all_datasets[d["pipeline"]], **d}
        else:
            missing.append(d["pipeline"])

    # add the total resource count
    dataset_resource_counts = resources_by_dataset()
    for d in dataset_resource_counts:
        if all_datasets.get(d["pipeline"]):
            all_datasets[d["pipeline"]] = {**all_datasets[d["pipeline"]], **d}
        else:
            missing.append(d["pipeline"])

    # add the first and last resource dates
    dataset_resource_dates = first_and_last_resource()
    for d in dataset_resource_dates:
        if all_datasets.get(d["pipeline"]):
            all_datasets[d["pipeline"]] = {**all_datasets[d["pipeline"]], **d}
        else:
            missing.append(d["pipeline"])

    # add the most recent endpoint entry date
    dataset_endpoint_entry_date = latest_endpoint_entry_date()
    for d in dataset_endpoint_entry_date:
        if all_datasets.get(d["pipeline"]):
            all_datasets[d["pipeline"]] = {**all_datasets[d["pipeline"]], **d}
        else:
            missing.append(d["pipeline"])

    # add endpoints coverage
    dataset_endpoint_entry_date = active_and_total_endpoints()
    for d in dataset_endpoint_entry_date:
        if all_datasets.get(d["pipeline"]):
            all_datasets[d["pipeline"]] = {**all_datasets[d["pipeline"]], **d}
        else:
            missing.append(d["pipeline"])

    # add typology
    dataset_endpoint_entry_date = get_typology()
    for d in dataset_endpoint_entry_date:
        if all_datasets.get(d["pipeline"]):
            all_datasets[d["pipeline"]] = {**all_datasets[d["pipeline"]], **d}
        else:
            missing.append(d["pipeline"])

    # add frequency of updates
    dataset_frequency = get_frequency()
    for d in dataset_frequency:
        if all_datasets.get(d["pipeline"]):
            all_datasets[d["pipeline"]] = {**all_datasets[d["pipeline"]], **d}
        else:
            missing.append(d["pipeline"])

    return all_datasets


def get_monthly_resource_counts(pipeline=None):
    if not pipeline:
        sql = """SELECT
              strftime('%Y-%m', resource.start_date) AS yyyy_mm,
              count(distinct resource.resource) AS count
            FROM
              resource
            WHERE
              resource.start_date != ""
            GROUP BY
              yyyy_mm
            ORDER BY
              yyyy_mm"""

    else:
        sql = """
            SELECT
              strftime('%Y-%m', resource.start_date) AS yyyy_mm,
              count(distinct resource.resource) AS count
            FROM
              resource
              INNER JOIN resource_endpoint ON resource.resource = resource_endpoint.resource
              INNER JOIN endpoint ON resource_endpoint.endpoint = endpoint.endpoint
              INNER JOIN source ON resource_endpoint.endpoint = source.endpoint
              INNER JOIN source_pipeline ON source.source = source_pipeline.source
            WHERE
              resource.start_date != ""
              AND source_pipeline.pipeline = :pipeline
            GROUP BY
              yyyy_mm
            ORDER BY
              yyyy_mm"""

    if pipeline:
        rows = get_datasette_query("digital-land", sql, {"pipeline": pipeline})
    else:
        rows = get_datasette_query("digital-land", sql)
    columns = rows.columns.tolist()

    return [dict(zip(columns, row)) for row in rows.to_numpy()]


def get_monthly_counts(pipeline=None):
    source_counts = get_monthly_source_counts(pipeline)
    resource_counts = get_monthly_resource_counts(pipeline)

    # handle if either are empty
    if not bool(source_counts):
        return None

    first_source_month_str = source_counts[0]["yyyy_mm"]
    first_resource_month_str = (
        resource_counts[0]["yyyy_mm"] if bool(resource_counts) else this_month()
    )

    earliest = (
        first_source_month_str
        if first_source_month_str < first_resource_month_str
        else first_resource_month_str
    )
    start_date = datetime.datetime.strptime(earliest, "%Y-%m")
    months_since_start = months_since(start_date)
    all_months = month_dict(months_since_start)

    counts = {}
    for k, v in {"resources": resource_counts, "sources": source_counts}.items():
        d = all_months.copy()
        for row in v:
            if row["yyyy_mm"] in d.keys():
                d[row["yyyy_mm"]] = d[row["yyyy_mm"]] + row["count"]
        # needs to be in tuple form
        counts[k] = [(k, v) for k, v in d.items()]
    counts["months"] = list(all_months.keys())

    return counts


def get_new_resources(dates=[yesterday(string=True)]):
    if len(dates) == 1:
        sql = f"""SELECT
            DISTINCT resource, start_date
            FROM resource
            WHERE start_date = '{dates[0]}'
            ORDER BY start_date"""
    else:
        sql = """SELECT
                DISTINCT resource, start_date
                FROM resource
                WHERE start_date IN %(dates)s
                ORDER BY start_date
                """ % {
            "dates": tuple(dates)
        }

    rows = get_datasette_query("digital-land", sql)

    return rows


def publisher_counts(pipeline):
    # returns resource, active resource, endpoints, sources, active source, latest resource date and days since update

    sql = """
          SELECT
          organisation.name,
          source.organisation,
          organisation.end_date AS organisation_end_date,
          COUNT(DISTINCT resource.resource) AS resources,
          COUNT(
            DISTINCT CASE
              WHEN resource.end_date == '' THEN resource.resource
              WHEN strftime('%Y%m%d', resource.end_date) >= strftime('%Y%m%d', 'now') THEN resource.resource
            END
          ) AS active_resources,
          COUNT(DISTINCT resource_endpoint.endpoint) AS endpoints,
          COUNT(DISTINCT source.source) AS sources,
          COUNT(
            DISTINCT CASE
              WHEN source.end_date == '' THEN source.source
              WHEN strftime('%Y%m%d', source.end_date) >= strftime('%Y%m%d', 'now') THEN source.source
            END
          ) AS active_sources,
          MAX(resource.start_date),
          Cast (
            (
              julianday('now') - julianday(MAX(resource.start_date))
            ) AS INTEGER
          ) AS days_since_update
        FROM
          resource
          INNER JOIN resource_endpoint ON resource.resource = resource_endpoint.resource
          INNER JOIN endpoint ON resource_endpoint.endpoint = endpoint.endpoint
          INNER JOIN source ON resource_endpoint.endpoint = source.endpoint
          INNER JOIN source_pipeline ON source.source = source_pipeline.source
          INNER JOIN organisation ON replace(source.organisation, '-eng', '') = organisation.organisation
        WHERE
          source_pipeline.pipeline = :pipeline
        GROUP BY
          source.organisation"""

    rows = get_datasette_query("digital-land", sql, {"pipeline": pipeline})
    columns = rows.columns.tolist()

    organisations = [dict(zip(columns, row)) for row in rows.to_numpy()]

    return index_by("organisation", organisations)
