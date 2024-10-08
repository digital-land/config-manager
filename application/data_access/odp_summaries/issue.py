import pandas as pd

from application.data_access.datasette_utils import get_datasette_query
from application.data_access.odp_summaries.utils import get_provisions

SPATIAL_DATASETS = [
    "article-4-direction-area",
    "conservation-area",
    "listed-building-outline",
    "tree-preservation-zone",
    "tree",
]
DOCUMENT_DATASETS = [
    "article-4-direction",
    "conservation-area-document",
    "tree-preservation-order",
]

# Separate variable for all datasets as arbitrary ordering required
ALL_DATASETS = [
    "article-4-direction",
    "article-4-direction-area",
    "conservation-area",
    "conservation-area-document",
    "listed-building-outline",
    "tree-preservation-order",
    "tree-preservation-zone",
    "tree",
]

# Configs that are passed to the front end for the filters
DATASET_TYPES = [
    {"name": "Spatial", "id": "spatial"},
    {"name": "Document", "id": "document"},
]

COHORTS = [
    {"name": "RIPA Beta", "id": "RIPA-Beta"},
    {"name": "RIPA BOPS", "id": "RIPA-BOPS"},
    {"name": "ODP Track 1", "id": "ODP-Track1"},
    {"name": "ODP Track 2", "id": "ODP-Track2"},
    {"name": "ODP Track 3", "id": "ODP-Track3"},
    {"name": "ODP Track 4", "id": "ODP-Track4"},
]


def get_provision_summary(dataset_clause, offset):
    sql = f"""
    SELECT
        organisation,
        organisation_name,
        dataset,
        active_endpoint_count,
        error_endpoint_count,
        count_issue_error_internal,
        count_issue_error_external,
        count_issue_warning_internal,
        count_issue_warning_external,
        count_issue_notice_internal,
        count_issue_notice_external
    FROM
        provision_summary
    {dataset_clause}
    limit 1000 offset {offset}
    """
    return get_datasette_query("performance", sql)


def get_full_provision_summary(datasets):
    dataset_clause = "WHERE " + " or ".join(
        ("dataset = '" + str(dataset) + "'" for dataset in datasets)
    )

    pagination_incomplete = True
    offset = 0
    provision_summary_df_list = []
    while pagination_incomplete:
        provision_summary_df = get_provision_summary(dataset_clause, offset)
        provision_summary_df_list.append(provision_summary_df)
        pagination_incomplete = len(provision_summary_df) == 1000
        offset += 1000
    return pd.concat(provision_summary_df_list)


def get_odp_issue_summary(dataset_types, cohorts):
    if dataset_types == ["spatial"]:
        datasets = SPATIAL_DATASETS
    elif dataset_types == ["document"]:
        datasets = DOCUMENT_DATASETS
    else:
        datasets = ALL_DATASETS
    provisions_df = get_provisions(cohorts, COHORTS)
    provision_summary_df = get_full_provision_summary(datasets)
    provision_issue_df = provisions_df.merge(
        provision_summary_df, how="left", on="organisation"
    )
    provision_issue_df = provision_issue_df.sort_values(
        ["cohort_start_date", "cohort", "organisation_name"]
    )
    # Get list of organisations to iterate over
    organisation_cohorts_df = provision_issue_df.drop_duplicates(
        subset=["cohort", "organisation"]
    )

    rows = []
    for idx, df_row in organisation_cohorts_df.iterrows():
        row = []
        row.append({"text": df_row["cohort"], "classes": "reporting-table-cell"})
        row.append(
            {"text": df_row["organisation_name"], "classes": "reporting-table-cell"}
        )
        issues_df = provision_issue_df[
            provision_issue_df["organisation"] == df_row["organisation"]
        ]
        for dataset in datasets:
            issues_df_row = issues_df[issues_df["dataset"] == dataset]
            # Check if any endpoints exist
            if (
                issues_df_row["active_endpoint_count"].values[0] > 0
                or issues_df_row["error_endpoint_count"].values[0] > 0
            ):
                issues = []
                # Now check for individual issue severities
                if issues_df_row["error_endpoint_count"].values[0] > 0:
                    issues.insert(0, "Endpoint broken")
                    classes = "reporting-table-cell reporting-null-background"
                if (
                    issues_df_row["count_issue_warning_internal"].values[0] > 0
                    or issues_df_row["count_issue_warning_external"].values[0] > 0
                ):
                    issues.insert(0, "Warning")
                    classes = "reporting-table-cell reporting-medium-background"
                if (
                    issues_df_row["count_issue_error_internal"].values[0] > 0
                    or issues_df_row["count_issue_error_external"].values[0] > 0
                ):
                    issues.insert(0, "Error")
                    classes = "reporting-table-cell reporting-bad-background"
                if (
                    issues_df_row["count_issue_notice_internal"].values[0] > 0
                    or issues_df_row["count_issue_notice_external"].values[0] > 0
                ):
                    issues.insert(0, "Notice")
                    classes = "reporting-table-cell reporting-bad-background"

                if issues == []:
                    text = "No issues"
                    classes = "reporting-table-cell reporting-good-background"
                else:
                    text = ", ".join(issues)
            else:
                text = "No endpoint"
                classes = "reporting-table-cell reporting-null-background"

            row.append({"text": text, "classes": classes})
        rows.append(row)

    # Calculate overview stats

    # Total issues for percentage calculation
    total_issues = (
        provision_issue_df[
            [
                "count_issue_error_internal",
                "count_issue_error_external",
                "count_issue_warning_internal",
                "count_issue_warning_external",
                "count_issue_notice_internal",
                "count_issue_notice_external",
            ]
        ]
        .sum()
        .sum()
    )
    total_internal = 0
    total_external = 0

    stats_rows = []
    # Loop over severities counting internal, external, percentages
    for severity in ["warning", "error", "notice"]:
        internal = provision_issue_df[f"count_issue_{severity}_internal"].sum()
        total_internal += internal
        external = provision_issue_df[f"count_issue_{severity}_external"].sum()
        total_external += external
        total = internal + external
        total_percentage = str(int((total / total_issues) * 100)) + "%"
        classes = (
            "reporting-medium-background"
            if severity == "warning"
            else "reporting-bad-background"
        )
        stats_rows.append(
            [
                {
                    "text": severity.title(),
                    "classes": classes + " reporting-table-cell",
                },
                {
                    "text": total,
                    "classes": "reporting-table-cell",
                },
                {
                    "text": total_percentage,
                    "classes": "reporting-table-cell",
                },
                {
                    "text": internal,
                    "classes": "reporting-table-cell",
                },
                {
                    "text": external,
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

    endpoints_with_no_issues_count = len(
        provision_issue_df[
            (provision_issue_df["count_issue_error_internal"] == 0)
            & (provision_issue_df["count_issue_error_external"] == 0)
            & (provision_issue_df["count_issue_warning_internal"] == 0)
            & (provision_issue_df["count_issue_warning_external"] == 0)
            & (provision_issue_df["count_issue_notice_internal"] == 0)
            & (provision_issue_df["count_issue_notice_external"] == 0)
            & (provision_issue_df["active_endpoint_count"] > 0)
            & (provision_issue_df["error_endpoint_count"] == 0)
        ]
    )
    total_endpoints = len(
        provision_issue_df[provision_issue_df["active_endpoint_count"] > 0]
    )

    stats_headers = [
        {"text": "Issue Severity"},
        {"text": "Count"},
        {"text": "% Count"},
        {"text": "Internal"},
        {"text": "External"},
    ]
    headers = [
        {"text": "Cohort", "classes": "reporting-table-header"},
        {"text": "Organisation", "classes": "reporting-table-header"},
        *map(
            lambda dataset: {"text": dataset, "classes": "reporting-table-header"},
            datasets,
        ),
    ]
    params = {
        "cohorts": COHORTS,
        "dataset_types": DATASET_TYPES,
        "selected_dataset_types": dataset_types,
        "selected_cohorts": cohorts,
    }
    return {
        "rows": rows,
        "headers": headers,
        "stats_headers": stats_headers,
        "stats_rows": stats_rows,
        "endpoints_no_issues": {
            "count": endpoints_with_no_issues_count,
            "total_endpoints": total_endpoints,
        },
        "params": params,
    }


def get_issue_summary_by_issue_type(dataset_clause, offset):
    sql = f"""
    SELECT
        *
    FROM
        endpoint_dataset_issue_type_summary edits
    LEFT JOIN (
        SELECT endpoint, end_date as endpoint_end_date, latest_status,
                latest_exception, entry_date as endpoint_entry_date
        FROM endpoint_dataset_summary
    ) as eds on edits.endpoint = eds.endpoint
    {dataset_clause}
    and eds.endpoint_end_date = ''
    limit 1000 offset {offset}
    """
    return get_datasette_query("performance", sql)


def get_odp_issues_by_issue_type(dataset_types, cohorts):
    # Separate method for issue download as granularity required is different from the issue summary
    provisions_df = get_provisions(cohorts, COHORTS)
    if dataset_types == ["spatial"]:
        datasets = SPATIAL_DATASETS
    elif dataset_types == ["document"]:
        datasets = DOCUMENT_DATASETS
    else:
        datasets = ALL_DATASETS

    dataset_clause = "WHERE " + " or ".join(
        ("edits.dataset = '" + str(dataset) + "'" for dataset in datasets)
    )

    pagination_incomplete = True
    offset = 0
    provision_summary_df_list = []
    while pagination_incomplete:
        issue_summary_df = get_issue_summary_by_issue_type(dataset_clause, offset)
        provision_summary_df_list.append(issue_summary_df)
        pagination_incomplete = len(issue_summary_df) == 1000
        offset += 1000
    issue_summary_df = pd.concat(provision_summary_df_list)

    provision_issue_df = provisions_df.merge(
        issue_summary_df, how="inner", on=["organisation", "cohort"]
    )

    return provision_issue_df[
        [
            "organisation",
            "cohort",
            "organisation_name",
            "pipeline",
            "issue_type",
            "severity",
            "responsibility",
            "count_issues",
            "collection",
            "endpoint",
            "endpoint_url",
            "latest_status",
            "latest_exception",
            "resource",
            "latest_log_entry_date",
            "endpoint_entry_date",
            "endpoint_end_date",
            "resource_start_date",
            "resource_end_date",
        ]
    ]
