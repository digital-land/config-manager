from application.data_access.datasette_utils import get_datasette_query_dev


def get_issue_summary():
    issues_df = get_datasette_query_dev("performance/issue_summary")

    # Convert DataFrame to a list of dictionaries (rows)
    rows = issues_df.to_dict(orient="records")

    # Define severity counts structure
    issue_severity_counts = [
        {
            "display_severity": "No issues",
            "severity": "",
            "total_count_percentage": 0.0,
            "internal_count": 0,
            "external_count": 0,
            "total_count": 0,
            "classes": "reporting-good-background",
        },
        {
            "display_severity": "Warning",
            "severity": "warning",
            "total_count_percentage": 0.0,
            "internal_count": 0,
            "external_count": 0,
            "total_count": 0,
            "classes": "reporting-medium-background",
        },
        {
            "display_severity": "Error",
            "severity": "error",
            "total_count_percentage": 0.0,
            "internal_count": 0,
            "external_count": 0,
            "total_count": 0,
            "classes": "reporting-bad-background",
        },
        {
            "display_severity": "Notice",
            "severity": "notice",
            "total_count_percentage": 0.0,
            "internal_count": 0,
            "external_count": 0,
            "total_count": 0,
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
        if issue_severity["total_count"] > 0:
            issue_severity[
                "total_count_percentage"
            ] = f"{int((issue_severity['total_count'] / total_issues) * 100)}%"
            stats_rows.append(
                [
                    {
                        "text": issue_severity["display_severity"],
                        "classes": issue_severity["classes"] + " reporting-table-cell",
                    },
                    {
                        "text": issue_severity["total_count"],
                        "classes": "reporting-table-cell",
                    },
                    {
                        "text": issue_severity["total_count_percentage"],
                        "classes": "reporting-table-cell",
                    },
                    {
                        "text": issue_severity["internal_count"],
                        "classes": "reporting-table-cell",
                    },
                    {
                        "text": issue_severity["external_count"],
                        "classes": "reporting-table-cell",
                    },
                ]
            )

    # Add totals row
    stats_rows.append(
        [
            {"text": "Total", "classes": "reporting-table-cell"},
            {"text": total_issues, "classes": "reporting-table-cell"},
            {"text": "", "classes": "reporting-table-cell"},
            {"text": total_internal, "classes": "reporting-table-cell"},
            {"text": total_external, "classes": "reporting-table-cell"},
        ]
    )

    # Define headers
    stats_headers = [
        {"text": "Issue Severity"},
        {"text": "Count"},
        {"text": "% Count"},
        {"text": "Internal"},
        {"text": "External"},
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
