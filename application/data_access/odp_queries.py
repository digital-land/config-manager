import tempfile

import pandas as pd

from application.data_access.datasette_utils import get_datasette_query

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

# Configs to pass to front end

DATASET_TYPES = [
    {"name": "Spatial", "id": "spatial"},
    {"name": "Document", "id": "document"},
]

COHORTS = [
    {"name": "RIPA BOPS", "id": "RIPA-BOPS"},
    {"name": "ODP Track 1", "id": "ODP-Track1"},
    {"name": "ODP Track 2", "id": "ODP-Track2"},
    {"name": "ODP Track 3", "id": "ODP-Track3"},
    {"name": "ODP Track 4", "id": "ODP-Track4"},
]


def get_odp_status_summary(dataset_types, cohorts):
    filtered_cohorts = [
        x for x in cohorts if cohorts[0] in [cohort["id"] for cohort in COHORTS]
    ]
    cohort_clause = (
        "where "
        + " or ".join(("odp_orgs.cohort = '" + str(n) + "'" for n in filtered_cohorts))
        if filtered_cohorts
        else ""
    )
    sql = f"""
        select
            odp_orgs.organisation,
            odp_orgs.cohort,
            odp_orgs.name,
            rle.collection,
            rle.pipeline,
            rle.endpoint,
            rle.endpoint_url,
            rle.status,
            rle.exception,
            rle.resource,
            rle.latest_log_entry_date,
            rle.endpoint_entry_date,
            rle.endpoint_end_date,
            rle.resource_start_date,
            rle.resource_end_date
        from (
            select p.organisation, p.cohort, o.name, p.start_date from provision p
                inner join organisation o on o.organisation = p.organisation
                where "cohort" not like "RIPA-Beta" and "project" like "open-digital-planning"
            group by p.organisation
            order by p.start_date, p.cohort, o.name
        )
        as odp_orgs
        left join reporting_latest_endpoints rle on replace(rle.organisation, '-eng', '') = odp_orgs.organisation
        {cohort_clause}
    """
    status_df = get_datasette_query("digital-land", sql)
    rows = []
    if status_df is not None:
        organisation_cohort_dict_list = (
            status_df[["organisation", "cohort", "name"]]
            .drop_duplicates()
            .to_dict(orient="records")
        )
        if dataset_types == ["spatial"]:
            datasets = SPATIAL_DATASETS
        elif dataset_types == ["document"]:
            datasets = DOCUMENT_DATASETS
        else:
            datasets = ALL_DATASETS
        for organisation_cohort_dict in organisation_cohort_dict_list:
            rows.append(
                create_row(
                    organisation_cohort_dict["organisation"],
                    organisation_cohort_dict["cohort"],
                    organisation_cohort_dict["name"],
                    status_df,
                    datasets,
                )
            )

        # Calculate overview stats
        percentages = 0.0
        datasets_added = 0
        for row in rows:
            percentages += float(row[-1]["text"].strip("%")) / 100
            for cell in row:
                if cell.get("data", None) and cell["text"] != "No endpoint":
                    datasets_added += 1
        datasets_added = str(datasets_added)
        max_datasets = len(rows) * len(datasets)
        average_percentage_added = str(int(100 * (percentages / len(rows))))[:2] + "%"

        headers = [
            {"text": "Cohort", "classes": "reporting-table-header"},
            {"text": "Organisation", "classes": "reporting-table-header"},
            *map(
                lambda dataset: {"text": dataset, "classes": "reporting-table-header"},
                datasets,
            ),
            {"text": "% provided", "classes": "reporting-table-header"},
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
            "percentage_datasets_added": average_percentage_added,
            "datasets_added": datasets_added,
            "max_datasets": max_datasets,
            "params": params,
        }

    else:
        return None


def create_row(organisation, cohort, name, status_df, datasets):
    row = []
    row.append({"text": cohort, "classes": "reporting-table-cell"})
    row.append({"text": name, "classes": "reporting-table-cell"})
    provided_score = 0
    for dataset in datasets:
        df_row = status_df[
            (status_df["organisation"] == organisation)
            & (status_df["pipeline"] == dataset)
        ]
        if len(df_row) != 0:
            provided_score += 1
            if df_row["status"].values:
                status = df_row["status"].values[0]
            else:
                # Look at exception for status
                if df_row["status"].values:
                    status = df_row["status"].values[0]
        else:
            status = "None"

        if status == "200":
            text = "Yes"
            classes = "reporting-good-background reporting-table-cell"
        elif (
            status != "None" and status != "200" and df_row["endpoint"].values[0] != ""
        ):
            text = "Yes - erroring"
            classes = "reporting-bad-background reporting-table-cell"
        else:
            text = "No endpoint"
            classes = "reporting-null-background reporting-table-cell"

        row.append(
            {
                "text": text,
                "classes": classes,
                "data": df_row.fillna("").to_dict(orient="records")
                if (len(df_row) != 0)
                else {},
            }
        )
    # Calculate % of endpoints provided
    provided_percentage = str(int(provided_score / len(datasets) * 100)) + "%"
    row.append({"text": provided_percentage, "classes": "reporting-table-cell"})
    return row


def get_odp_issue_summary(dataset_types, cohorts):
    filtered_cohorts = [
        x for x in cohorts if cohorts[0] in [cohort["id"] for cohort in COHORTS]
    ]
    cohort_clause = (
        "WHERE "
        + " or ".join(("odp_orgs.cohort = '" + str(n) + "'" for n in filtered_cohorts))
        if filtered_cohorts
        else ""
    )
    sql = f"""
    SELECT
    odp_orgs.organisation,
    odp_orgs.cohort,
    odp_orgs.name,
    case
        when (it.severity = 'info') then ''
        else it.severity
    end as severity,
    COUNT(case
        when it.severity != 'info' then 1
        else null
        end
    ) as severity_count,
    COUNT(
        case
        when it.responsibility = 'internal' and it.severity != 'info' then 1
        else null
        end
    ) as internal_responsibility_count,
    COUNT(
        case
        when it.responsibility = 'external' and it.severity != 'info' then 1
        else null
        end
    ) as external_responsibility_count,
    rle.collection,
    rle.pipeline,
    rle.endpoint,
    rle.endpoint_url,
    rle.status,
    rle.exception,
    rle.resource,
    rle.latest_log_entry_date,
    rle.endpoint_entry_date,
    rle.endpoint_end_date,
    rle.resource_start_date,
    rle.resource_end_date
    FROM (
        SELECT
            p.organisation,
            p.cohort,
            o.name,
            p.start_date
        FROM
            provision p
        INNER JOIN
            organisation o ON o.organisation = p.organisation
        WHERE
            p.cohort != 'RIPA-Beta' AND
            p.project = 'open-digital-planning'
        GROUP BY
            p.organisation,
            p.cohort,
            o.name,
            p.start_date
    ) AS odp_orgs
    LEFT JOIN
        reporting_latest_endpoints rle ON REPLACE(rle.organisation, '-eng', '') = odp_orgs.organisation
    LEFT JOIN
        issue i ON rle.resource = i.resource
    LEFT JOIN
        issue_type it ON i.issue_type = it.issue_type
    {cohort_clause}
    GROUP BY
        odp_orgs.organisation,
        it.severity,
        rle.pipeline
    ORDER BY
        odp_orgs.start_date,
        odp_orgs.cohort,
        odp_orgs.name;
    """
    issues_df = get_datasette_query("digital-land", sql)
    rows = []
    if issues_df is not None:
        organisation_cohort_dict_list = (
            issues_df[["organisation", "cohort", "name"]]
            .drop_duplicates()
            .to_dict(orient="records")
        )
        if dataset_types == ["spatial"]:
            datasets = SPATIAL_DATASETS
        elif dataset_types == ["document"]:
            datasets = DOCUMENT_DATASETS
        else:
            datasets = ALL_DATASETS
        for organisation_cohort_dict in organisation_cohort_dict_list:
            rows.append(
                create_issue_row(
                    organisation_cohort_dict["organisation"],
                    organisation_cohort_dict["cohort"],
                    organisation_cohort_dict["name"],
                    issues_df,
                    datasets,
                )
            )

        # Overview Stats
        # Dict to store helpful metrics
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
        # Metric for how many endpoints have issues with each severity
        total_issues = 0
        endpoints_with_no_issues_count = 0
        total_endpoints = 0
        for row in rows:
            for cell in row:
                text = cell.get("text", None)
                if text is not None or text != "":
                    data = cell.get("data", {})
                    if data != {}:
                        total_endpoints += 1
                        for issue_severity in issue_severity_counts:
                            if issue_severity["display_severity"] in text:
                                for line in data:
                                    severity = line["severity"]
                                    if (
                                        severity != ""
                                        and severity == issue_severity["severity"]
                                    ):
                                        issue_severity["internal_count"] += line[
                                            "internal_responsibility_count"
                                        ]
                                        issue_severity["external_count"] += line[
                                            "external_responsibility_count"
                                        ]
                                        issue_severity["total_count"] += (
                                            line["internal_responsibility_count"]
                                            + line["external_responsibility_count"]
                                        )
                                        total_issues += (
                                            line["internal_responsibility_count"]
                                            + line["external_responsibility_count"]
                                        )
                                    elif (
                                        severity == ""
                                        and severity == issue_severity["severity"]
                                    ):
                                        endpoints_with_no_issues_count += 1

        stats_rows = []
        # Compute totals/percentages
        total_internal = 0
        total_external = 0
        for issue_severity in issue_severity_counts:
            issue_severity["total_count_percentage"] = (
                str(int((issue_severity["total_count"] / total_issues) * 100)) + "%"
            )

            total_internal += issue_severity["internal_count"]
            total_external += issue_severity["external_count"]

            # Write all the metrics to row except for No issues
            if issue_severity["severity"] != "":
                stats_rows.append(
                    [
                        {
                            "text": issue_severity["display_severity"],
                            "classes": issue_severity["classes"]
                            + " reporting-table-cell",
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
            "issue_severity_counts": issue_severity_counts,
            "stats_headers": stats_headers,
            "stats_rows": stats_rows,
            "endpoints_no_issues": {
                "count": endpoints_with_no_issues_count,
                "total_endpoints": total_endpoints,
            },
            "params": params,
        }

    else:
        return None


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
            present_severities = [i for i in present_severities if i != ""]
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
                "data": df_rows.fillna("").to_dict(orient="records")
                if (len(df_rows) != 0)
                else {},
            }
        )
    return row


def generate_odp_summary_csv(odp_summary):
    dfs = []
    for row in odp_summary["rows"]:
        for cell in row:
            try:
                data = cell["data"]
                dfs.append(pd.DataFrame.from_records(data))
            except Exception:
                pass
    output_df = pd.concat(dfs)
    with tempfile.NamedTemporaryFile(mode="w", delete=False) as csvfile:
        output_df.to_csv(csvfile, index=False)
        return csvfile.name
