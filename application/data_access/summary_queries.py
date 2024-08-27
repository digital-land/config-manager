import random
from datetime import datetime, timedelta, timezone

import pandas as pd

from application.data_access.datasette_utils import (
    generate_weeks,
    get_datasette_query,
    get_datasette_query_issue_summary_test,
)


def get_contributions_and_errors(offset):
    sql = f"""
        select
            count(*) as count,
            case when (status = '200') then '200' else 'Not 200' end as status,
            strftime('%Y',entry_date) as year,
            strftime('%m',entry_date) as month,
            strftime('%W',entry_date) as week,
            strftime('%d',entry_date) as day,
            substr(entry_date,1,10) as entry_date
        from log l
        where year > '2018'
        group by substr(entry_date,1,10), case when status = '200' then status else 'not_200' end
        limit 1000 offset {offset}
    """
    return get_datasette_query("digital-land", sql)


def get_contributions_and_errors_by_day():
    # Use pagination in case rows returned > 1000
    pagination_incomplete = True
    offset = 0
    contributions_and_errors_df_list = []
    while pagination_incomplete:
        contributions_and_errors_df = get_contributions_and_errors(offset)
        contributions_and_errors_df_list.append(contributions_and_errors_df)
        pagination_incomplete = len(contributions_and_errors_df) == 1000
        offset += 1000
    contributions_and_errors_df = pd.concat(contributions_and_errors_df_list)
    return contributions_and_errors_df


def get_issue_counts():
    sql = """
        select
            it.severity, count(*) as count
        from issue i
        inner join issue_type it
        where i.issue_type = it.issue_type
        group by it.severity
    """
    issues_df = get_datasette_query("digital-land", sql)
    if issues_df is not None:
        errors = issues_df[issues_df["severity"] == "error"].iloc[0]["count"]
        warning = issues_df[issues_df["severity"] == "warning"].iloc[0]["count"]
        return errors, warning
    else:
        return None


def get_internal_issues_by_week():
    # SQL query to get only rows where responsibility = 'internal'
    sql = """SELECT count_issues,
  date,
  responsibility FROM issue_summary WHERE responsibility = 'internal'
  """

    data_df = get_datasette_query_issue_summary_test("performance", sql)
    # print(data_df)

    dummy_data = {
        "count_issues": [
            random.randint(20, 500) for _ in range(100)
        ],  # Random issue count between 20 and 500
        "date": [
            datetime(2024, random.randint(1, 12), random.randint(1, 28)).strftime(
                "%d-%m-%Y"
            )
            for _ in range(100)
        ],  # Random dates in 2024
    }

    print("==================== DUMMY DATE ================")
    print(dummy_data)

    issues_df = pd.DataFrame(data_df)

    print(issues_df)
    if issues_df.empty:
        print("No data returned.")
        return None

    # Convert 'date' column to datetime format
    issues_df["date"] = pd.to_datetime(issues_df["date"], format="%d-%m-%Y")

    # week and year
    issues_df["week"] = issues_df["date"].dt.isocalendar().week
    issues_df["year"] = issues_df["date"].dt.year

    # Filter for 2024
    issues_df_2024 = issues_df[issues_df["year"] == 2024]

    # Group year and week -> sum issues per week
    weekly_issues = (
        issues_df_2024.groupby(["year", "week"])["count_issues"]
        .sum()
        .reset_index(name="issue_count")
    )

    # create DataFrame list up to next week in 2024
    current_week = datetime.now(timezone.utc).isocalendar()[1]
    all_weeks = pd.DataFrame(
        {"week": range(1, current_week + 1), "year": [2024] * current_week}
    )

    # Merge with the weekly_issues to ensure every week is present
    all_weeks_issues = pd.merge(
        all_weeks, weekly_issues, on=["year", "week"], how="left"
    )
    all_weeks_issues["issue_count"].fillna(0, inplace=True)  # 0 if no issue :)

    # issue_count to int because it's a float
    all_weeks_issues["issue_count"] = all_weeks_issues["issue_count"].astype(int)

    # set start date as start of this year (check what we wan the start date to be!)
    start_date = datetime(2024, 1, 1)

    # Convert week and year to the start date of the week (casting to int to avoid numpy.int64 issue,
    # again probably won't need this with acrual data)
    all_weeks_issues["date"] = all_weeks_issues.apply(
        lambda x: (start_date + timedelta(weeks=int(x["week"]) - 1)).strftime(
            "%d-%m-%Y"
        ),
        axis=1,
    )

    # DataFrame to the desired format
    result = (
        all_weeks_issues[["date", "issue_count"]]
        .rename(columns={"issue_count": "count"})
        .to_dict(orient="records")
    )

    return result


def get_contributions_and_erroring_endpoints(contributions_and_errors_df):
    if contributions_and_errors_df is not None:
        contributions_df = contributions_and_errors_df[
            contributions_and_errors_df["status"] == "200"
        ].reindex()
        errors_df = contributions_and_errors_df[
            contributions_and_errors_df["status"] != "200"
        ].reindex()
        contributions = contributions_df["count"].tolist()
        contributions_dates = contributions_df["entry_date"].tolist()
        errors = errors_df["count"].tolist()
        errors_dates = errors_df["entry_date"].tolist()
        return {"dates": contributions_dates, "contributions": contributions}, {
            "dates": errors_dates,
            "errors": errors,
        }
    else:
        return None


def get_endpoints_added_by_week():
    sql = """
        select
            strftime('%Y',entry_date) as year,
            strftime('%m',entry_date) as month,
            strftime('%W',entry_date) as week,
            strftime('%d',entry_date) as day,
            count(endpoint),
            substr(entry_date,1,10) as entry_date
        from endpoint
        where year > '2018'
        group by year, week
    """
    endpoints_added_df = get_datasette_query("digital-land", sql)
    if endpoints_added_df is not None:
        endpoints_added_df["week"] = pd.to_numeric(endpoints_added_df["week"])
        endpoints_added_df["year"] = pd.to_numeric(endpoints_added_df["year"])
        # min_entry_date = endpoints_added_df["entry_date"].min()
        # Use hardcoded start date as data does not begin at the same date across all three graphs
        min_entry_date = "2019-08-21"
        dates = generate_weeks(date_from=min_entry_date)
        endpoints_added = []
        for date in dates:
            current_date_data_df = endpoints_added_df[
                (endpoints_added_df["week"] == date["week_number"])
                & (endpoints_added_df["year"] == date["year_number"])
            ]
            if len(current_date_data_df) != 0:
                count = current_date_data_df["count(endpoint)"].values[0]
            else:
                count = 0
            endpoints_added.append(
                {"date": date["date"].strftime("%d/%m/%Y"), "count": count}
            )
        return endpoints_added
    else:
        return None


def get_endpoint_errors_and_successes_by_week(contributions_and_errors_by_day_df):
    contributions_and_errors_by_day_df["year"] = pd.to_numeric(
        contributions_and_errors_by_day_df["year"]
    )
    contributions_and_errors_by_day_df["week"] = pd.to_numeric(
        contributions_and_errors_by_day_df["week"]
    )
    contributions_and_errors_by_week_df = (
        contributions_and_errors_by_day_df.groupby(["year", "week", "status"])["count"]
        .sum()
        .reset_index()
    )
    min_entry_date = contributions_and_errors_by_day_df["entry_date"].min()
    dates = generate_weeks(date_from=min_entry_date)
    successes_by_week = []
    successes_percentages_by_week = []
    errors_percentages_by_week = []
    for date in dates:
        current_date_data_df = contributions_and_errors_by_week_df[
            (contributions_and_errors_by_week_df["week"] == date["week_number"])
            & (contributions_and_errors_by_week_df["year"] == date["year_number"])
        ]
        if len(current_date_data_df) > 0:
            successes_df = current_date_data_df[current_date_data_df["status"] == "200"]
            if len(successes_df) > 0:
                successes = successes_df["count"].values[0]
            else:
                successes = 0
            errors_df = current_date_data_df[
                current_date_data_df["status"] == "Not 200"
            ]
            if len(errors_df) > 0:
                errors = errors_df["count"].values[0]
            else:
                errors = 0
            if errors == 0 and successes == 0:
                success_percentage = 0
                error_percentage = 0
            else:
                success_percentage = successes / (successes + errors) * 100
                error_percentage = errors / (successes + errors) * 100
        else:
            successes = 0
            errors = 0
        successes_by_week.append(
            {"date": date["date"].strftime("%d/%m/%Y"), "count": successes}
        )
        successes_percentages_by_week.append(
            {"date": date["date"].strftime("%d/%m/%Y"), "count": success_percentage}
        )
        errors_percentages_by_week.append(
            {"date": date["date"].strftime("%d/%m/%Y"), "count": error_percentage}
        )
    return successes_by_week, successes_percentages_by_week, errors_percentages_by_week
