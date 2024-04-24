import numpy as np
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


def get_endpoint_resource_data():
    sql = (
        """
        select re.*, s.licence
        from reporting_latest_endpoints re
        left join (
            select distinct endpoint, licence from source where length(licence) < 12
        ) s on re.endpoint = s.endpoint
    """,
    )

    endpoint_resource_df = get_datasette_query("digital-land", sql)

    return endpoint_resource_df


def get_provisions():
    sql = """
        SELECT
          cohort,
          notes,
          organisation,
          project,
          provision_reason,
          start_date
        FROM
          provision p
        WHERE
          cohort IN (
            "ODP-Track1",
            "ODP-Track3",
            "ODP-Track2",
            "RIPA-BOPS"
          )
          AND provision_reason = "expected"
          AND p.project == "open-digital-planning"
        GROUP BY
          organisation
        ORDER BY
          cohort
        """
    provision_df = get_datasette_query("digital-land", sql)
    provision_df["organisation"] = provision_df["organisation"].str.replace(
        ":", "-eng:"
    )
    return provision_df


def get_issue_types_by_severity(severity_list):
    sql = """
    select issue_type, severity, responsibility
    from issue_type
    """
    all_issue_types_df = get_datasette_query("digital-land", sql)
    df = all_issue_types_df.loc[all_issue_types_df["severity"].isin(severity_list)]
    return df


def get_fields_for_resource(resource, dataset):
    sql = f"""
    select f.field, fr.resource
    from
        fact_resource fr
        inner join fact f on fr.fact = f.fact
    where
        resource = '{resource}'
    group by
        f.field
    """

    facts_df = get_datasette_query(dataset, sql)
    return facts_df


def get_column_mappings_for_resource(resource, dataset):
    sql = f"""
    select column, field
    from
        column_field
    where
        resource = '{resource}'
    """
    column_field_df = get_datasette_query(dataset, sql)
    return column_field_df


def get_all_issues_for_resource(resource, dataset):
    sql = f"""
    select field, issue_type, count(*) as count_issues
    from issue
    where resource = '{resource}'
    group by field, issue_type
    """

    issues_df = get_datasette_query(dataset, sql)
    return issues_df


# generic function to try the resource datasette queries
# will return a df with resource and dataset fields as keys, and query results as other fields
def try_results(function, resource, dataset):
    # try grabbing results
    try:
        df = function(resource, dataset)

        df["resource"] = resource
        df["dataset"] = dataset
    # if error record resource and dataset, other fields will be given NaNs in concat
    except Exception:
        df = pd.DataFrame({"resource": [resource], "dataset": [dataset]})

    return df


def get_odp_compliance_summary(dataset_types, cohorts):
    # filtered_cohorts = [
    #     x for x in cohorts if cohorts[0] in [cohort["id"] for cohort in COHORTS]
    # ]
    # cohort_clause = (
    #     "where "
    #     + " or ".join(("odp_orgs.cohort = '" + str(n) + "'" for n in filtered_cohorts))
    #     if filtered_cohorts
    #     else ""
    # )
    if dataset_types == ["spatial"]:
        datasets = SPATIAL_DATASETS
    elif dataset_types == ["document"]:
        datasets = DOCUMENT_DATASETS
    else:
        datasets = ALL_DATASETS
    print(datasets)
    # dataset_clause = (
    #     "and "
    #     + " or ".join(("rle.pipeline = '" + str(n) + "'" for n in datasets))
    #     if filtered_cohorts
    #     else ""
    # )
    provision_df = get_provisions()

    endpoint_resource_df = get_endpoint_resource_data()

    print(len(endpoint_resource_df))
    print(endpoint_resource_df)

    # This filter is not working for some reason ??
    # filter to valid, active endpoints and resources
    endpoint_resource_filtered_df = endpoint_resource_df[
        # (endpoint_resource_df["pipeline"].isin(datasets)) &
        (endpoint_resource_df["status"] == 200)
        # &
        # (endpoint_resource_df["endpoint_end_date"].isnull()) &
        # (endpoint_resource_df["resource_end_date"].isnull())
    ].copy()

    print(len(endpoint_resource_filtered_df))

    # join to provision and bring through cohort and start date
    endpoint_resource_filtered_df = endpoint_resource_filtered_df.merge(
        provision_df, how="inner", on="organisation"
    )

    # replace licence NaNs with blank
    endpoint_resource_filtered_df["licence"].replace(np.nan, "", inplace=True)

    # table of unique resources and pipelines
    resource_df = (
        endpoint_resource_filtered_df[["pipeline", "resource"]]
        .drop_duplicates()
        .dropna(axis=0)
    )
    print(len(resource_df))

    issue_severity_lookup = get_issue_types_by_severity(["error"])

    # get results for col mappings and fields in arrays
    results_col_map = [
        try_results(get_column_mappings_for_resource, r["resource"], r["pipeline"])
        for index, r in resource_df.iterrows()
    ]
    results_field_resource = [
        try_results(get_fields_for_resource, r["resource"], r["pipeline"])
        for index, r in resource_df.iterrows()
    ]
    results_issues = [
        try_results(get_all_issues_for_resource, r["resource"], r["pipeline"])
        for index, r in resource_df.iterrows()
    ]

    # concat the results, resources which errored with have NaNs in query results fields
    results_col_map_df = pd.concat(results_col_map)
    results_field_resource_df = pd.concat(results_field_resource)
    results_issues_df = pd.concat(results_issues)

    # join on severity to issues
    results_issues_df = results_issues_df.merge(
        issue_severity_lookup, how="inner", on="issue_type"
    )

    # filter to just errors and get a unique list of fields with errors per dataset and resource
    resource_issue_errors_df = results_issues_df[
        ["dataset", "resource", "field"]
    ].drop_duplicates()

    # add in match field for column mappings (lowering so that we don't exclude case mis-matches)
    results_col_map_df["field_matched"] = np.where(
        (results_col_map_df["field"].isin(["geometry", "point"]))
        | (
            results_col_map_df["field"].str.lower()
            == results_col_map_df["column"].str.lower()
        ),
        1,
        0,
    )

    # add in flag for fields supplied (i.e. they're in the mapping table)
    results_col_map_df["field_supplied"] = 1

    # add in flag for fields present
    results_field_resource_df["field_loaded"] = 1

    # add in flag for fields with errors
    resource_issue_errors_df["field_errors"] = 1

    # in issues table reference not being supplied is linked to entity. we don't count entity as a supplied
    # field (it is removed further down), so here it is re-mapped to reference field so it's included as an error
    resource_issue_errors_df["field"] = resource_issue_errors_df["field"].replace(
        "entity", "reference"
    )

    dataset_field_df = pd.read_csv(
        "https://raw.githubusercontent.com/digital-land/specification/main/specification/dataset-field.csv"
    )

    # rename pipeline to dataset in endpoint_resource table
    endpoint_resource_filtered_df.rename(columns={"pipeline": "dataset"}, inplace=True)

    # left join from endpoint resource table to all the fields that each dataset should have
    resource_spec_fields_df = endpoint_resource_filtered_df[
        [
            "organisation",
            "name",
            "cohort",
            "dataset",
            "endpoint",
            "status",
            "latest_log_entry_date",
            "endpoint_entry_date",
            "resource",
            "licence",
        ]
    ].merge(dataset_field_df[["dataset", "field"]], on="dataset")

    # join on field loaded flag for each resource and field
    resource_fields_match = resource_spec_fields_df.merge(
        results_field_resource_df[["dataset", "resource", "field", "field_loaded"]],
        how="left",
        on=["dataset", "resource", "field"],
    )

    # join on field supplied and matched flag for each resource and field
    resource_fields_map_match = resource_fields_match.merge(
        results_col_map_df[
            ["dataset", "resource", "field", "field_supplied", "field_matched"]
        ],
        how="left",
        on=["dataset", "resource", "field"],
    )

    # join on field errors flag for each resource and field
    resource_fields_map_issues = resource_fields_map_match.merge(
        resource_issue_errors_df, how="left", on=["dataset", "resource", "field"]
    )

    # remove fields that are auto-created in the pipeline from final table to avoid mis-counting
    # ("entity", "organisation", "prefix", "point" for all but tree, and "entity", "organisation", "prefix" for tree)
    resource_fields_scored = resource_fields_map_issues[
        (
            (resource_fields_map_issues["dataset"] != "tree")
            & (
                ~resource_fields_map_issues["field"].isin(
                    ["entity", "organisation", "prefix", "point"]
                )
            )
            | (resource_fields_map_issues["dataset"] == "tree")
            & (
                ~resource_fields_map_issues["field"].isin(
                    ["entity", "organisation", "prefix"]
                )
            )
        )
    ]

    # where entry-date hasn't been supplied it is auto-created -
    # change field_loaded to NaN in these instances so we don't count it as a loaded field
    entry_date_mask = (
        (resource_fields_scored["field"] == "entry-date")
        & (resource_fields_scored["field_supplied"].isnull())
        & (resource_fields_scored["field_loaded"] == 1)
    )

    resource_fields_scored.loc[entry_date_mask, "field_loaded"] = np.nan
    resource_fields_scored = resource_fields_scored.replace(np.nan, 0)

    # group by and aggregate for final summaries
    final_count = (
        resource_fields_scored.groupby(
            [
                "organisation",
                "name",
                "cohort",
                "dataset",
                "endpoint",
                "licence",
                "resource",
                "status",
                "latest_log_entry_date",
                "endpoint_entry_date",
            ]
        )
        .agg(
            {
                "field": "count",
                "field_supplied": "sum",
                "field_matched": "sum",
                "field_loaded": "sum",
                "field_errors": "sum",
            }
        )
        .reset_index()
        .sort_values(["name"])
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
    csv_out_cols = [
        "organisation",
        "name",
        "cohort",
        "dataset",
        "endpoint",
        "licence",
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
    final_count[csv_out_cols].to_csv("report_conformance_organisation-dataset.csv")
    final_count.head()
    return ""
