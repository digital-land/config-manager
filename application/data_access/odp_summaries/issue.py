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
        name,
        dataset,
        active_endpoint_count,
        error_endpoint_count,
        count_internal_error,
        count_external_error,
        count_internal_warning,
        count_external_warning,
        count_internal_notice,
        count_external_notice
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
    provision_issue_df.to_csv("test.csv")
    # Get list of organisations to iterate over
    organisation_cohorts_df = provision_issue_df.drop_duplicates(
        subset=["cohort", "organisation"]
    )

    rows = []
    for idx, df_row in organisation_cohorts_df.iterrows():
        row = []
        row.append({"text": df_row["cohort"], "classes": "reporting-table-cell"})
        row.append({"text": df_row["name"], "classes": "reporting-table-cell"})
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
                if (
                    issues_df_row["count_internal_warning"].values[0] > 0
                    or issues_df_row["count_external_warning"].values[0] > 0
                ):
                    issues.append("Warning")
                    classes = "reporting-table-cell reporting-medium-background"
                if issues_df_row["error_endpoint_count"].values[0] > 0:
                    issues.append("Endpoint error")
                    classes = "reporting-table-cell reporting-null-background"
                if (
                    issues_df_row["count_internal_error"].values[0] > 0
                    or issues_df_row["count_external_error"].values[0] > 0
                ):
                    issues.append("Error")
                    classes = "reporting-table-cell reporting-bad-background"
                if (
                    issues_df_row["count_internal_notice"].values[0] > 0
                    or issues_df_row["count_external_notice"].values[0] > 0
                ):
                    issues.append("Notice")
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
                "count_internal_error",
                "count_external_error",
                "count_internal_warning",
                "count_external_warning",
                "count_internal_notice",
                "count_external_notice",
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
        internal = provision_issue_df[f"count_internal_{severity}"].sum()
        total_internal += internal
        external = provision_issue_df[f"count_external_{severity}"].sum()
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
            (provision_issue_df["count_internal_error"] == 0)
            & (provision_issue_df["count_external_error"] == 0)
            & (provision_issue_df["count_internal_warning"] == 0)
            & (provision_issue_df["count_external_warning"] == 0)
            & (provision_issue_df["count_internal_notice"] == 0)
            & (provision_issue_df["count_external_notice"] == 0)
            & (provision_issue_df["active_endpoint_count"] > 0)
        ]
    )
    total_endpoints = provision_issue_df["active_endpoint_count"].sum()

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


def create_issue_row(organisation, cohort, name, issue_df, datasets):
    row = []
    row.append({"text": cohort, "classes": "reporting-table-cell"})
    row.append({"text": name, "classes": "reporting-table-cell"})
    for dataset in datasets:
        df_rows = issue_df[
            (issue_df["organisation"] == organisation)
            & (issue_df["pipeline"] == dataset)
        ]
        if len(df_rows) != 0:
            present_severities = df_rows["severity"].tolist()
            # Filter out blank/info severities
            present_severities = [
                i for i in present_severities if (i != "" and i is not None)
            ]
            if present_severities == [] or present_severities == [None]:
                text = "No issues"
                classes = "reporting-table-cell reporting-good-background"
            elif "error" in present_severities or "notice" in present_severities:
                text = ", ".join(
                    severity.capitalize() for severity in present_severities
                )
                classes = "reporting-table-cell reporting-bad-background"
            else:
                text = ", ".join(
                    severity.capitalize() for severity in present_severities
                )
                classes = "reporting-table-cell reporting-medium-background"
        else:
            text = "No endpoint"
            classes = "reporting-table-cell reporting-null-background"

        row.append(
            {
                "text": text,
                "classes": classes,
                "data": (
                    df_rows.fillna("").to_dict(orient="records")
                    if (len(df_rows) != 0)
                    else {}
                ),
            }
        )
    return row


def get_odp_issues_by_issue_type(dataset_types, cohorts):
    # Separate method for issue download as granularity required is different from the issue summary
    filtered_cohorts = [
        x for x in cohorts if cohorts[0] in [cohort["id"] for cohort in COHORTS]
    ]
    cohort_clause = (
        "and ("
        + " or ".join(
            ("odp_orgs.cohort = '" + str(cohort) + "'" for cohort in filtered_cohorts)
        )
        + ")"
        if filtered_cohorts
        else ""
    )
    if dataset_types == ["spatial"]:
        dataset_clause = (
            "and ("
            + " or ".join(
                (
                    "rle.pipeline = '" + str(dataset) + "'"
                    for dataset in SPATIAL_DATASETS
                )
            )
            + ")"
        )
    elif dataset_types == ["document"]:
        dataset_clause = (
            "and ("
            + " or ".join(
                (
                    "rle.pipeline = '" + str(dataset) + "'"
                    for dataset in DOCUMENT_DATASETS
                )
            )
            + ")"
        )
    else:
        dataset_clause = (
            "and ("
            + " or ".join(
                ("rle.pipeline = '" + str(dataset) + "'" for dataset in ALL_DATASETS)
            )
            + ")"
        )
    sql = f"""
    SELECT
        odp_orgs.organisation,
        odp_orgs.cohort,
        odp_orgs.name,
        rle.pipeline,
        case
            when (it.severity = 'info') then ''
            else i.issue_type
        end as issue_type,
        case
            when (it.severity = 'info') then ''
            else it.severity
        end as severity,
        it.responsibility,
        COUNT(
            case
            when it.severity != 'info' then 1
            else null
            end
        ) as count,
        rle.collection,
        rle.endpoint,
        rle.endpoint_url,
        rle.resource,
        rle.latest_log_entry_date,
        rle.endpoint_entry_date,
        rle.endpoint_end_date,
        rle.resource_start_date,
        rle.resource_end_date
    FROM
    (
        SELECT
            p.organisation,
            p.cohort,
            o.name,
            c.start_date
        FROM
            provision p
        INNER JOIN
            organisation o ON o.organisation = p.organisation
        INNER JOIN
            cohort c on p.cohort = c.cohort
        WHERE
            p.project = "open-digital-planning"
        GROUP BY
            p.organisation,
            p.cohort,
            o.name,
            p.start_date
    ) AS odp_orgs
    LEFT JOIN reporting_latest_endpoints rle ON REPLACE(rle.organisation, '-eng', '') = odp_orgs.organisation
    LEFT JOIN issue i ON rle.resource = i.resource and rle.pipeline = i.dataset
    LEFT JOIN issue_type it ON i.issue_type = it.issue_type
    WHERE
        it.severity != 'info'
        {cohort_clause}
        {dataset_clause}
    GROUP BY
        odp_orgs.organisation,
        rle.pipeline,
        i.issue_type
    ORDER BY
        odp_orgs.start_date,
        odp_orgs.cohort,
        odp_orgs.name;
    """
    issues_df = get_datasette_query("digital-land", sql)
    return issues_df
