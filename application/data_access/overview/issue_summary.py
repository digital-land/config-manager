import pandas as pd

from application.data_access.datasette_utils import get_datasette_query


def get_full_issue_summary():
    pagination_incomplete = True
    offset = 0
    issue_summary_df_list = []
    while pagination_incomplete:
        issue_summary_df = get_issue_summary(offset)
        issue_summary_df_list.append(issue_summary_df)
        pagination_incomplete = len(issue_summary_df) == 1000
        offset += 1000
    issues_df = pd.concat(issue_summary_df_list)

    # Convert DataFrame to a list of dictionaries (rows)
    rows = issues_df.to_dict(orient="records")

    # Define severity counts structure
    issue_severity_counts = [
        {
            "display_severity": "No issues",
            "severity": "",
            "total_count": 0,
            "total_count_percentage": 0.00,
            "internal_count": 0,
            "internal_count_percentage": 0.00,
            "external_count": 0,
            "external_count_percentage": 0.00,
            "classes": "reporting-good-background",
        },
        {
            "display_severity": "Info",
            "severity": "info",
            "total_count": 0,
            "total_count_percentage": 0.00,
            "internal_count": 0,
            "internal_count_percentage": 0.00,
            "external_count": 0,
            "external_count_percentage": 0.00,
            "classes": "reporting-good-background",
        },
        {
            "display_severity": "Warning",
            "severity": "warning",
            "total_count": 0,
            "total_count_percentage": 0.00,
            "internal_count": 0,
            "internal_count_percentage": 0.00,
            "external_count": 0,
            "external_count_percentage": 0.00,
            "classes": "reporting-medium-background",
        },
        {
            "display_severity": "Error",
            "severity": "error",
            "total_count": 0,
            "total_count_percentage": 0.00,
            "internal_count": 0,
            "internal_count_percentage": 0.00,
            "external_count": 0,
            "external_count_percentage": 0.00,
            "classes": "reporting-bad-background",
        },
        {
            "display_severity": "Notice",
            "severity": "notice",
            "total_count": 0,
            "total_count_percentage": 0.00,
            "internal_count": 0,
            "internal_count_percentage": 0.00,
            "external_count": 0,
            "external_count_percentage": 0.00,
            "classes": "reporting-bad-background",
        },
    ]

    # Calculate metrics from rows
    total_issues = 0
    endpoints_with_no_issues_count = 0
    total_endpoints = len(rows)

    for row in rows:
        severity = row.get("severity", "")
        issues_count = row.get("count_issues", 0)
        responsibility = row.get("responsibility", "")
        internal_count = 0
        external_count = 0

        # Accumulate severity count
        if responsibility == "internal":
            internal_count += int(issues_count)
        elif responsibility == "external":
            external_count += int(issues_count)
        total_count = internal_count + external_count
        if severity:
            for issue_severity in issue_severity_counts:
                if issue_severity["severity"] == severity:
                    issue_severity["internal_count"] += int(internal_count)
                    issue_severity["external_count"] += int(external_count)
                    issue_severity["total_count"] += total_count
                    total_issues += total_count
        else:
            endpoints_with_no_issues_count += 1

    # Compute totals/percentages
    total_internal = sum(issue["internal_count"] for issue in issue_severity_counts)
    total_external = sum(issue["external_count"] for issue in issue_severity_counts)

    # Add issue_severity row
    stats_rows = []
    for issue_severity in issue_severity_counts:
        if issue_severity["internal_count"] > 0:
            issue_severity["internal_count_percentage"] = round(
                (issue_severity["internal_count"] / total_issues) * 100, 2
            )

        if issue_severity["external_count"] > 0:
            issue_severity["external_count_percentage"] = round(
                (issue_severity["external_count"] / total_issues) * 100, 2
            )

        if issue_severity["total_count"] > 0:
            issue_severity["total_count_percentage"] = round(
                (issue_severity["total_count"] / total_issues) * 100, 2
            )

            stats_rows.append(
                [
                    {
                        "text": issue_severity["display_severity"],
                        "classes": issue_severity["classes"] + " reporting-table-cell",
                    },
                    {
                        "text": f"{issue_severity['internal_count']} ({issue_severity['internal_count_percentage']}%)",
                        "classes": "reporting-table-cell",
                    },
                    {
                        "text": f"{issue_severity['external_count']} ({issue_severity['external_count_percentage']}%)",
                        "classes": "reporting-table-cell",
                    },
                    {
                        "text": f"{issue_severity['total_count']} ({issue_severity['total_count_percentage']}%)",
                        "classes": "reporting-table-cell",
                    },
                ]
            )

    # Add totals row, the bottom
    stats_rows.append(
        [
            {"text": "Total", "classes": "reporting-table-cell"},
            {
                "text": f"{total_internal} ({round((total_internal/total_issues)*100, 2)}%)",
                "classes": "reporting-table-cell",
            },
            {
                "text": f"{total_external} ({round((total_external/total_issues)*100, 2)}%)",
                "classes": "reporting-table-cell",
            },
            {"text": total_issues, "classes": "reporting-table-cell"},
        ]
    )

    # Define headers
    stats_headers = [
        {"text": "Severity"},
        {"text": "Internal (%)"},
        {"text": "External (%)"},
        {"text": "Total (%)"},
    ]

    return {
        "issue_severity_counts": issue_severity_counts,
        "stats_headers": stats_headers,
        "stats_rows": stats_rows,
        "endpoints_no_issues": {
            "count": endpoints_with_no_issues_count,
            "total_endpoints": total_endpoints,
        },
    }


def get_full_issue_summary_for_csv():
    pagination_incomplete = True
    offset = 0
    issue_summary_df_list = []
    while pagination_incomplete:
        issue_summary_df = get_issue_summary_for_csv(offset)
        issue_summary_df_list.append(issue_summary_df)
        pagination_incomplete = len(issue_summary_df) == 1000
        offset += 1000
    issue_summary_df = pd.concat(issue_summary_df_list)

    issue_summary_df = issue_summary_df[issue_summary_df["count_issues"].notna()]

    return issue_summary_df[
        [
            "organisation",
            "organisation_name",
            "pipeline",
            "issue_type",
            "severity",
            "responsibility",
            "count_issues",
            "collection",
            "endpoint",
            "endpoint_url",
            "resource",
            "latest_log_entry_date",
            "resource_start_date",
            "resource_end_date",
        ]
    ]


def get_issue_summary(offset):
    sql = f"""
    select count_issues, severity, responsibility from endpoint_dataset_issue_type_summary limit 1000 offset {offset}
    """
    return get_datasette_query("performance", sql)


def get_issue_summary_for_csv(offset):
    sql = f"""
    select * from endpoint_dataset_issue_type_summary limit 1000 offset {offset}
    """
    return get_datasette_query("performance", sql)
