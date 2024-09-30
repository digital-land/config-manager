import ast

import numpy as np
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


def get_column_field_summary(dataset_clause, offset):
    sql = f"""
    select * from column_field_summary
    where resource != ''
    and ({dataset_clause})
    limit 1000 offset {offset}
    """
    column_field_df = get_datasette_query("performance", sql)

    return column_field_df


def get_issue_summary(dataset_clause, offset):
    sql = f"""
    select  * from issue_summary
    where ({dataset_clause})
    limit 1000 offset {offset}
    """
    issue_summary_df = get_datasette_query("performance", sql)

    return issue_summary_df


def get_odp_conformance_summary(dataset_types, cohorts):
    params = {
        "cohorts": COHORTS,
        "dataset_types": DATASET_TYPES,
        "selected_dataset_types": dataset_types,
        "selected_cohorts": cohorts,
    }
    if dataset_types == ["spatial"]:
        datasets = SPATIAL_DATASETS
    elif dataset_types == ["document"]:
        datasets = DOCUMENT_DATASETS
    else:
        datasets = ALL_DATASETS
    dataset_clause = " or ".join(
        ("pipeline = '" + str(dataset) + "'" for dataset in datasets)
    )

    provision_df = get_provisions(cohorts, COHORTS)

    # Download column field summary table
    # Use pagination in case rows returned > 1000
    pagination_incomplete = True
    offset = 0
    column_field_df_list = []
    while pagination_incomplete:
        column_field_df = get_column_field_summary(dataset_clause, offset)
        column_field_df_list.append(column_field_df)
        pagination_incomplete = len(column_field_df) == 1000
        offset += 1000
    if len(column_field_df_list) == 0:
        return {"params": params, "rows": [], "headers": []}
    column_field_df = pd.concat(column_field_df_list)

    column_field_df = pd.merge(
        column_field_df, provision_df, on=["organisation", "cohort"], how="inner"
    )

    # Download issue summary table
    pagination_incomplete = True
    offset = 0
    issue_df_list = []
    while pagination_incomplete:
        issue_df = get_issue_summary(dataset_clause, offset)
        issue_df_list.append(issue_df)
        pagination_incomplete = len(issue_df) == 1000
        offset += 1000
    issue_df = pd.concat(issue_df_list)

    dataset_field_df = get_dataset_field()

    # remove fields that are auto-created in the pipeline from the dataset_field file to avoid mis-counting
    # ("entity", "organisation", "prefix", "point" for all but tree, and "entity", "organisation", "prefix" for tree)
    dataset_field_df = dataset_field_df[
        (dataset_field_df["dataset"] != "tree")
        & (
            ~dataset_field_df["field"].isin(
                ["entity", "organisation", "prefix", "point"]
            )
        )
        | (dataset_field_df["dataset"] == "tree")
        & (~dataset_field_df["field"].isin(["entity", "organisation", "prefix"]))
    ]

    # Filter out fields not in spec
    column_field_df["matching_field"] = column_field_df.replace({'"', ""}).apply(
        lambda row: [
            field
            for field in (
                row["matching_field"].split(",") if row["matching_field"] else ""
            )
            if field
            in dataset_field_df[dataset_field_df["dataset"] == row["dataset"]][
                "field"
            ].values
        ],
        axis=1,
    )
    column_field_df["non_matching_field"] = column_field_df.replace({'"', ""}).apply(
        lambda row: [
            field
            for field in (
                row["non_matching_field"].split(",")
                if row["non_matching_field"]
                else ""
            )
            if field
            in dataset_field_df[dataset_field_df["dataset"] == row["dataset"]][
                "field"
            ].values
        ],
        axis=1,
    )

    # Map entity errors to reference field
    issue_df["fields"] = issue_df["fields"].replace("entity", "reference")
    # Filter out issues for fields not in dataset field (specification)
    issue_df["fields"] = issue_df.replace({'"', ""}).apply(
        lambda row: [
            field
            for field in (row["fields"].split(",") if row["fields"] else "")
            if field
            in dataset_field_df[dataset_field_df["dataset"] == row["dataset"]][
                "field"
            ].values
        ],
        axis=1,
    )

    # Create field matched and field supplied scores
    column_field_df["field_matched"] = column_field_df.apply(
        lambda row: len(row["matching_field"]) if row["matching_field"] else 0, axis=1
    )
    column_field_df["field_supplied"] = column_field_df.apply(
        lambda row: row["field_matched"]
        + (len(row["non_matching_field"]) if row["non_matching_field"] else 0),
        axis=1,
    )
    column_field_df["field"] = column_field_df.apply(
        lambda row: len(
            dataset_field_df[dataset_field_df["dataset"] == row["dataset"]]
        ),
        axis=1,
    )

    # Check for fields which have error issues
    results_issues = [
        issue_df[
            (issue_df["resource"] == r["resource"]) & (issue_df["severity"] == "error")
        ]
        for index, r in column_field_df.iterrows()
    ]
    results_issues_df = pd.concat(results_issues)

    # Create fields with errors column
    column_field_df["field_errors"] = column_field_df.apply(
        lambda row: len(
            results_issues_df[row["resource"] == results_issues_df["resource"]]
        ),
        axis=1,
    )

    # Create endpoint ID column to track multiple endpoints per organisation-dataset
    column_field_df["endpoint_no."] = (
        column_field_df.groupby(["organisation", "dataset"]).cumcount() + 1
    )
    column_field_df["endpoint_no."] = column_field_df["endpoint_no."].astype(str)

    # group by and aggregate for final summaries
    final_count = (
        column_field_df.groupby(
            [
                "organisation",
                "name",
                "cohort",
                "dataset",
                "endpoint",
                "endpoint_no.",
                "resource",
                "status",
                "latest_log_entry_date",
                "endpoint_entry_date",
                "cohort_start_date",
            ]
        )
        .agg(
            {
                "field": "sum",
                "field_supplied": "sum",
                "field_matched": "sum",
                "field_errors": "sum",
            }
        )
        .reset_index()
    )

    final_count["field_error_free"] = (
        final_count["field_supplied"] - final_count["field_errors"]
    )
    final_count["field_error_free"] = final_count["field_error_free"].replace(-1, 0)

    # add string fields for [n fields]/[total fields] style counts
    final_count["field_supplied_count"] = (
        final_count["field_supplied"].astype(int).map(str)
        + "/"
        + final_count["field"].map(str)
    )
    final_count["field_error_free_count"] = (
        final_count["field_error_free"].astype(int).map(str)
        + "/"
        + final_count["field"].map(str)
    )
    final_count["field_matched_count"] = (
        final_count["field_matched"].astype(int).map(str)
        + "/"
        + final_count["field"].map(str)
    )

    # create % columns
    final_count["field_supplied_pct"] = (
        final_count["field_supplied"] / final_count["field"]
    )
    final_count["field_error_free_pct"] = (
        final_count["field_error_free"] / final_count["field"]
    )
    final_count["field_matched_pct"] = (
        final_count["field_matched"] / final_count["field"]
    )

    final_count.reset_index(drop=True, inplace=True)
    final_count.sort_values(
        ["cohort_start_date", "cohort", "name", "dataset"], inplace=True
    )

    out_cols = [
        "cohort",
        "name",
        "organisation",
        "dataset",
        "endpoint_no.",
        "field_supplied_count",
        "field_supplied_pct",
        "field_matched_count",
        "field_matched_pct",
    ]

    csv_out_cols = [
        "organisation",
        "name",
        "cohort",
        "dataset",
        "endpoint",
        "endpoint_no.",
        "resource",
        "status",
        "latest_log_entry_date",
        "endpoint_entry_date",
        "field",
        "field_supplied",
        "field_matched",
        "field_errors",
        "field_error_free",
        "field_supplied_pct",
        "field_error_free_pct",
        "field_matched_pct",
    ]

    headers = [
        *map(
            lambda column: {
                "text": make_pretty(column).title(),
                "classes": "reporting-table-header",
            },
            out_cols,
        )
    ]

    rows = [
        [
            {
                "text": make_pretty(cell),
                "classes": "reporting-table-cell " + get_background_class(cell),
            }
            for cell in r
        ]
        for index, r in final_count[out_cols].iterrows()
    ]

    # Calculate overview stats
    overview_datasets = [
        "article-4-direction-area",
        "conservation-area",
        "listed-building-outline",
        "tree",
        "tree-preservation-zone",
    ]
    overview_stats_df = pd.DataFrame()
    overview_stats_df["dataset"] = overview_datasets
    overview_stats_df = overview_stats_df.merge(
        final_count[["dataset", "field_supplied_pct"]][
            final_count["field_supplied_pct"] < 0.5
        ]
        .groupby("dataset")
        .count(),
        on="dataset",
        how="left",
    ).rename(columns={"field_supplied_pct": "< 50%"})
    overview_stats_df = overview_stats_df.merge(
        final_count[["dataset", "field_supplied_pct"]][
            (final_count["field_supplied_pct"] >= 0.5)
            & (final_count["field_supplied_pct"] < 0.8)
        ]
        .groupby("dataset")
        .count(),
        on="dataset",
        how="left",
    ).rename(columns={"field_supplied_pct": "50% - 80%"})
    overview_stats_df = overview_stats_df.merge(
        final_count[["dataset", "field_supplied_pct"]][
            final_count["field_supplied_pct"] >= 0.8
        ]
        .groupby("dataset")
        .count(),
        on="dataset",
        how="left",
    ).rename(columns={"field_supplied_pct": "> 80%"})
    overview_stats_df.replace(np.nan, 0, inplace=True)
    overview_stats_df = overview_stats_df.astype(
        {
            "< 50%": int,
            "50% - 80%": int,
            "> 80%": int,
        }
    )

    stats_headers = [
        *map(
            lambda column: {
                "text": column.title(),
                "classes": "reporting-table-header",
            },
            overview_stats_df.columns.values,
        )
    ]
    stats_rows = [
        [{"text": cell, "classes": "reporting-table-cell"} for cell in r]
        for index, r in overview_stats_df.iterrows()
    ]
    return {
        "headers": headers,
        "rows": rows,
        "stats_headers": stats_headers,
        "stats_rows": stats_rows,
        "params": params,
    }, final_count[csv_out_cols]


def make_pretty(text):
    if type(text) is float:
        # text is a float, make a percentage
        return str((round(100 * text))) + "%"
    elif "_" in text:
        # text is a column name
        return text.replace("_", " ").replace("pct", "%").replace("count", "")
    return text


def get_background_class(text):
    if type(text) is float:
        group = int((text * 100) / 10)
        if group == 10:
            return "reporting-100-background"
        else:
            return "reporting-" + str(group) + "0-" + str(group + 1) + "0-background"
    return ""


def get_dataset_field():
    specification_df = pd.read_csv(
        "https://raw.githubusercontent.com/digital-land/specification/main/specification/specification.csv"
    )
    rows = []
    for index, row in specification_df.iterrows():
        specification_dicts = ast.literal_eval(row["json"])
        for dict in specification_dicts:
            dataset = dict["dataset"]
            fields = [field["field"] for field in dict["fields"]]
            for field in fields:
                rows.append({"dataset": dataset, "field": field})
    return pd.DataFrame(rows)
