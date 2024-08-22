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


def get_datasette_query(
    db, sql, filter=None, url="https://datasette.planning.data.gov.uk"
):
    url = f"{url}/{db}.json"
    params = {"sql": sql, "_shape": "array", "_size": "max"}
    if filter:
        params.update(filter)
    try:
        http = get_datasette_http()
        resp = http.get(url, params=params)
        resp.raise_for_status()
        df = pd.DataFrame.from_dict(resp.json())
        return df
    except Exception as e:
        logging.warning(e)
        return None


def get_datasette_query_dev(
    db, filter=None, url="https://datasette.development.digital-land.info"
):
    url = f"{url}/{db}.json"
    params = {}

    if filter:
        params.update(filter)

    try:
        http = get_datasette_http()
        all_rows = []

        while True:
            """
            Datasette returns a max of 1000 rows. This should be able to be changed but for now,
            if there is more than 1000 rows, a pagination next will be returned in the response.
            We can use this to fetch the next 1000 rows repeatedly until all rows have been accumulated.
            """

            resp = http.get(url, params=params)
            response_json = resp.json()
            rows = response_json.get("rows", [])

            # Accumulate rows
            all_rows.extend(rows)

            # Check if there's a "next" token for pagination
            next_token = response_json.get("next")
            if not next_token:
                break

            params["_next"] = next_token

        if all_rows and response_json.get("columns"):
            df = pd.DataFrame(all_rows, columns=response_json["columns"])
            return df
        else:
            logging.error("No rows or columns available to create a DataFrame")
            return None

    except Exception as e:
        logging.warning(f"Exception occurred: {e}")
        return None


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


def generate_weeks(number_of_weeks=None, date_from=None):
    now = datetime.datetime.now()
    monday = now - datetime.timedelta(days=now.weekday())
    dates = []

    if date_from:
        date = datetime.datetime.strptime(date_from, "%Y-%m-%d")
        while date < now:
            week_number = int(date.strftime("%W"))
            year_number = int(date.year)
            dates.append(
                {"date": date, "week_number": week_number, "year_number": year_number}
            )
            date = date + datetime.timedelta(days=7)
        return dates
    elif number_of_weeks:
        for week in range(0, number_of_weeks):
            date = monday - datetime.timedelta(weeks=week)
            week_number = int(date.strftime("%W"))
            year_number = int(date.year)
            dates.append(
                {"date": date, "week_number": week_number, "year_number": year_number}
            )
        return list(reversed(dates))
