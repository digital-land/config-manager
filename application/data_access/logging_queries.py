import os

import duckdb
import pandas as pd

s3Bucket = "s3://development-reporting"
frontEndPageViewPath = "application/development-data-val-fe/PageView"

aws_access_key_id = os.getenv("aws_access_key_id")
aws_secret_access_key = os.getenv("aws_secret_access_key")

"""
    the pageView table will have the following schema:
        {"name": "timestamp", "type": "TIMESTAMP"},
        {"name": "sessionId", "type": "VARCHAR"},
        {"name": "pageRoute", "type": "VARCHAR"}
"""

parquet_files = f"{s3Bucket}/{frontEndPageViewPath}/2024-02-26.parquet"


def getDuckdbConnection():
    # create a duckdb connection
    con = duckdb.connect()
    con.execute("SET home_directory='/tmp'")
    con.install_extension("https")
    con.install_extension("aws")
    con.load_extension("https")
    con.load_extension("aws")

    # set the aws credentials
    con.execute("SET s3_region='eu-west-2';")
    con.execute(f"SET s3_access_key_id='{aws_access_key_id}';")
    con.execute(f"SET s3_secret_access_key='{aws_secret_access_key}';")

    print("duckdb aws credentials set to: ", aws_access_key_id, aws_secret_access_key)

    return con


def getStartPageViews():
    con = getDuckdbConnection()

    con.execute(
        f'CREATE TABLE Requests AS SELECT * FROM read_parquet("{parquet_files}")'
    )

    page_views_by_week = pd.DataFrame(
        con.execute(
            """
        SELECT DATE_TRUNC('week', timestamp) AS week, COUNT(DISTINCT sessionId)
        FROM Requests
        WHERE pageRoute = '/dataset'
        GROUP BY week
        ORDER BY week
    """
        ).fetchall(),
        columns=["week", "count"],
    )

    # Define your date range
    start = "2024-01-01"  # replace with your start date
    end = "2024-03-04"  # replace with your end date

    # Create a DataFrame that includes all weeks in the date range
    all_weeks = pd.DataFrame(
        pd.date_range(start=start, end=end, freq="W"), columns=["week"]
    )
    all_weeks["week"] = all_weeks["week"].dt.to_period("W").dt.start_time

    # Convert 'week' column to datetime
    page_views_by_week["week"] = pd.to_datetime(page_views_by_week["week"])

    # Merge the two DataFrames
    page_views_by_week = pd.merge(all_weeks, page_views_by_week, on="week", how="left")

    # Replace NaN values with 0
    page_views_by_week["count"].fillna(0, inplace=True)

    return page_views_by_week


def getErrorsPageViews():
    con = getDuckdbConnection()

    con.execute(
        f'CREATE TABLE Requests AS SELECT * FROM read_parquet("{parquet_files}")'
    )

    page_views_by_week = pd.DataFrame(
        con.execute(
            """
        SELECT DATE_TRUNC('week', timestamp) AS week, COUNT(DISTINCT sessionId)
        FROM Requests
        WHERE pageRoute = '/errors'
        GROUP BY week
        ORDER BY week
    """
        ).fetchall(),
        columns=["week", "count"],
    )

    # Define your date range
    start = "2024-01-01"  # replace with your start date
    end = "2024-03-04"  # replace with your end date

    # Create a DataFrame that includes all weeks in the date range
    all_weeks = pd.DataFrame(
        pd.date_range(start=start, end=end, freq="W"), columns=["week"]
    )
    all_weeks["week"] = all_weeks["week"].dt.to_period("W").dt.start_time

    # Convert 'week' column to datetime
    page_views_by_week["week"] = pd.to_datetime(page_views_by_week["week"])

    # Merge the two DataFrames
    page_views_by_week = pd.merge(all_weeks, page_views_by_week, on="week", how="left")

    # Replace NaN values with 0
    page_views_by_week["count"].fillna(0, inplace=True)

    return page_views_by_week


def getConfirmationPageViews():
    con = getDuckdbConnection()

    con.execute(
        f'CREATE TABLE Requests AS SELECT * FROM read_parquet("{parquet_files}")'
    )

    page_views_by_week = pd.DataFrame(
        con.execute(
            """
        SELECT DATE_TRUNC('week', timestamp) AS week, COUNT(DISTINCT sessionId)
        FROM Requests
        WHERE pageRoute = '/confirmation'
        GROUP BY week
        ORDER BY week
    """
        ).fetchall(),
        columns=["week", "count"],
    )

    # Define your date range
    start = "2024-01-01"  # replace with your start date
    end = "2024-03-04"  # replace with your end date

    # Create a DataFrame that includes all weeks in the date range
    all_weeks = pd.DataFrame(
        pd.date_range(start=start, end=end, freq="W"), columns=["week"]
    )
    all_weeks["week"] = all_weeks["week"].dt.to_period("W").dt.start_time

    # Convert 'week' column to datetime
    page_views_by_week["week"] = pd.to_datetime(page_views_by_week["week"])

    # Merge the two DataFrames
    page_views_by_week = pd.merge(all_weeks, page_views_by_week, on="week", how="left")

    # Replace NaN values with 0
    page_views_by_week["count"].fillna(0, inplace=True)

    return page_views_by_week


# get all rows of the table
def getAllPageViews():
    con = getDuckdbConnection()

    # load the parquet file
    con.execute(
        f'CREATE TABLE Requests AS SELECT * FROM read_parquet("{parquet_files}")'
    )

    page_views_by_week = con.execute(
        """
        SELECT *
        FROM Requests
    """
    ).fetchall()

    return page_views_by_week


# get the average time take for each session from the start page to the confirmation page
def getAverageTimeToConfirmation():
    con = getDuckdbConnection()

    con.execute(
        f'CREATE TABLE Requests AS SELECT * FROM read_parquet("{parquet_files}")'
    )

    page_views_by_week = pd.DataFrame(
        con.execute(
            """
            SELECT sessionId, MIN(timestamp) AS start, MAX(timestamp) AS end
            FROM Requests
            WHERE pageRoute = '/dataset'
            OR pageRoute = '/confirmation'
            GROUP BY sessionId
        """
        ).fetchall(),
        columns=["sessionId", "start", "end"],
    )

    page_views_by_week["time"] = page_views_by_week["end"] - page_views_by_week["start"]

    return page_views_by_week["time"].mean()
