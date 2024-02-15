import datetime
import logging

import pandas as pd
import requests
from requests import adapters
from urllib3 import Retry


def get_datasette_http():
    """
    Function to return  http for the use of querying  datasette,
    specifically to add retries for larger queries
    """
    retry_strategy = Retry(total=3, status_forcelist=[400], backoff_factor=0)

    adapter = adapters.HTTPAdapter(max_retries=retry_strategy)

    http = requests.Session()
    http.mount("https://", adapter)
    http.mount("http://", adapter)

    return http


def get_datasette_query(db, sql, url="https://datasette.planning.data.gov.uk"):
    url = f"{url}/{db}.json"
    params = {"sql": sql, "_shape": "array"}
    try:
        http = get_datasette_http()
        resp = http.get(url, params=params)
        resp.raise_for_status()
        df = pd.DataFrame.from_dict(resp.json())
        return df
    except Exception as e:
        logging.warning(e)
        return None


def get_logs():
    year = datetime.datetime.today().year
    sql = f"""
        select year, month, week, day, status, count
        from (
            select
                status,
                strftime('%Y',entry_date) as year,
                strftime('%m',entry_date) as month,
                strftime('%W',entry_date) as week,
                strftime('%d',entry_date) as day,
                count(endpoint) as count
            from log
            group by year, week, status
        ) as t1
        where year = '{year}' or year = '{year-1}'
    """
    logs_df = get_datasette_query("digital-land", sql)
    if logs_df is not None:
        return logs_df
    else:
        return None


def get_number_of_contributions():
    current_date = datetime.datetime.now().date()
    date_query = f" where substr(l.entry_date, 1, 10) = '{current_date}'"
    sql = f"""
        select count(*) as count
            from log l
            {date_query}
            and l.status = 200
    """
    contributions_df = get_datasette_query("digital-land", sql)
    if contributions_df is not None:
        return int(contributions_df.iloc[0]["count"])
    else:
        return None


def get_number_of_erroring_endpoints():
    current_date = datetime.datetime.now().date()
    date_query = f" where substr(l.entry_date, 1, 10) = '{current_date}'"
    sql = f"""
        select count(*) as count
            from log l
            {date_query}
            and l.status != 200
    """
    errors_df = get_datasette_query("digital-land", sql)
    if errors_df is not None:
        return int(errors_df.iloc[0]["count"])
    else:
        return None


def get_endpoints_added_by_week():
    sql = """
        select
            strftime('%Y',entry_date) as year,
            strftime('%m',entry_date) as month,
            strftime('%W',entry_date) as week,
            strftime('%d',entry_date) as day,
            count(endpoint)
        from endpoint
        group by year, week
    """
    endpoints_added_df = get_datasette_query("digital-land", sql)
    year = datetime.datetime.today().year
    if endpoints_added_df is not None:
        current_year_endpoints_added_df = endpoints_added_df[
            endpoints_added_df["year"].astype(int) == year
        ]
        last_year_endpoints_added_df = endpoints_added_df[
            endpoints_added_df["year"].astype(int) == year - 1
        ]
        # Cast week numbers to int now to prevent repeatedly converting in loop
        current_year_endpoints_added_df["week"] = pd.to_numeric(
            current_year_endpoints_added_df["week"]
        )
        last_year_endpoints_added_df["week"] = pd.to_numeric(
            last_year_endpoints_added_df["week"]
        )
        dates = generate_dates(20)

        endpoints_added = []
        for date in dates:
            year = date["year_number"]
            week = date["week_number"]
            for df in [last_year_endpoints_added_df, current_year_endpoints_added_df]:
                df_year = int(df["year"].iloc[0])
                if year == df_year:
                    df_week_numbers = df["week"].tolist()
                    if week in df_week_numbers:
                        count = df.loc[df["week"] == week, "count(endpoint)"].values[0]
                    else:
                        count = 0
                    endpoints_added.append(
                        {"date": date["date"].strftime("%d/%m/%Y"), "count": count}
                    )

        return endpoints_added
    else:
        return None


def get_endpoint_errors_and_successes_by_week(logs_df):
    year = datetime.datetime.today().year
    current_year_endpoint_successes_df = logs_df[
        (logs_df["year"].astype(int) == year) & (logs_df["status"] == "200")
    ]
    last_year_endpoint_successes_df = logs_df[
        (logs_df["year"].astype(int) == year - 1) & (logs_df["status"] == "200")
    ]
    current_year_endpoint_errors_df = logs_df[
        (logs_df["year"].astype(int) == year) & (logs_df["status"] != "200")
    ]
    last_year_endpoint_errors_df = logs_df[
        (logs_df["year"].astype(int) == year - 1) & (logs_df["status"] != "200")
    ]

    current_year_endpoint_successes_df["week"] = pd.to_numeric(
        current_year_endpoint_successes_df["week"]
    )
    last_year_endpoint_successes_df["week"] = pd.to_numeric(
        last_year_endpoint_successes_df["week"]
    )
    current_year_endpoint_errors_df["week"] = pd.to_numeric(
        current_year_endpoint_errors_df["week"]
    )
    last_year_endpoint_errors_df["week"] = pd.to_numeric(
        last_year_endpoint_errors_df["week"]
    )

    dates = generate_dates(20)

    successes_by_week = []
    error_percentages_by_week = []
    for date in dates:
        year = date["year_number"]
        week = date["week_number"]
        for df in [last_year_endpoint_successes_df, current_year_endpoint_successes_df]:
            df_year = int(df["year"].iloc[0])
            if year == df_year:
                df_week_numbers = df["week"].tolist()
                if week in df_week_numbers:
                    successes = sum(df[(df["week"] == week)]["count"].tolist())
                else:
                    successes = 0
                successes_by_week.append(
                    {"date": date["date"].strftime("%d/%m/%Y"), "count": successes}
                )

        for df in [last_year_endpoint_errors_df, current_year_endpoint_errors_df]:
            df_year = int(df["year"].iloc[0])
            if year == df_year:
                df_week_numbers = df["week"].tolist()
                if week in df_week_numbers:
                    errors = sum(df[(df["week"] == week)]["count"].tolist())
                    error_percentage = 100 * errors / (errors + successes)
                else:
                    error_percentage = 0
                error_percentages_by_week.append(
                    {
                        "date": date["date"].strftime("%d/%m/%Y"),
                        "count": error_percentage,
                    }
                )

    return successes_by_week, error_percentages_by_week


# def get_overview():
#     logs_df = get_logs()
#     contributions = get_number_of_contributions()
#     errors = get_number_of_erroring_endpoints()
#     endpoints_added = get_endpoints_added_by_week()
#     endpoint_successes, endpoint_errors = get_endpoint_errors_and_successes_by_week(
#         logs_df
#     )
#     return {
#         "contributions": contributions,
#         "errors": errors,
#         "endpoints_added": endpoints_added,
#         "resources_downloaded": endpoint_successes,
#         "error_percentages": endpoint_errors,
#     }


# def get_unhealthy_endpoints()

# def get_datasets_summary():
#     # get all the datasets listed with their active status
#     all_datasets = index_by("dataset", get_datasets())
#     missing = []

#     # add the publisher coverage numbers
#     dataset_coverage = publisher_coverage()
#     for d in dataset_coverage:
#         if all_datasets.get(d["pipeline"]):
#             all_datasets[d["pipeline"]] = {**all_datasets[d["pipeline"]], **d}
#         else:
#             missing.append(d["pipeline"])

#     # add the total resource count
#     dataset_resource_counts = resources_by_dataset()
#     for d in dataset_resource_counts:
#         if all_datasets.get(d["pipeline"]):
#             all_datasets[d["pipeline"]] = {**all_datasets[d["pipeline"]], **d}
#         else:
#             missing.append(d["pipeline"])

#     # add the first and last resource dates
#     dataset_resource_dates = first_and_last_resource()
#     for d in dataset_resource_dates:
#         if all_datasets.get(d["pipeline"]):
#             all_datasets[d["pipeline"]] = {**all_datasets[d["pipeline"]], **d}
#         else:
#             missing.append(d["pipeline"])

#     return all_datasets


def generate_dates(number_of_weeks):
    now = datetime.datetime.now()
    monday = now - datetime.timedelta(days=now.weekday())
    dates = []
    for week in range(0, number_of_weeks):
        date = monday - datetime.timedelta(weeks=week)
        week_number = int(date.strftime("%W"))
        year_number = int(date.year)
        dates.append(
            {"date": date, "week_number": week_number, "year_number": year_number}
        )

    return list(reversed(dates))
