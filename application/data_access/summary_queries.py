import datetime

import pandas as pd

from application.data_access.datasette_utils import generate_weeks, get_datasette_query


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


def get_contributions_and_erroring_endpoints():
    current_date = datetime.datetime.now().date()
    date_query = (
        f" where year = '{current_date.year - 1}' or year = '{current_date.year}'"
    )
    sql = f"""
        select
            count(*) as count,
            status,
            substr(entry_date,1,10) as entry_date,
            strftime('%Y',entry_date) as year
            from log l
            {date_query}
            group by substr(entry_date,1,10), case when status = '200' then status else 'not_200' end
    """

    contributions_and_errors_df = get_datasette_query("digital-land", sql)
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
        dates = generate_weeks(20)

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

    dates = generate_weeks(20)

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
