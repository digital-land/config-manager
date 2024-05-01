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


def get_endpoint_resource_column_field_data(dataset_clause, offset):
    sql = (
        f"""
        select rle.*, s.licence, cf.column, cf.field
        from reporting_latest_endpoints rle
        left join (
            select distinct endpoint, licence from source where length(licence) < 12
        ) s on rle.endpoint = s.endpoint
        left join (
            select * from column_field
        ) cf on cf.resource = rle.resource
        where {dataset_clause}
        and status = '200'
        and rle.endpoint_end_date = ""
        and rle.resource_end_date = ""
        order by rle.organisation
        limit 1000 offset {offset}
    """,
    )

    endpoint_resource_df = get_datasette_query("digital-land", sql)

    return endpoint_resource_df


def get_endpoint_resource_issue_data(dataset_clause, offset):
    sql = (
        f"""
        select i.field, i.issue_type, count(*) as count_issues, rle.pipeline as dataset, i.resource
        from reporting_latest_endpoints rle
        left join (
            select distinct endpoint, licence from source where length(licence) < 12
        ) s on rle.endpoint = s.endpoint
        left join (
            select * from issue
        ) i on i.resource = rle.resource
        where {dataset_clause}
        and status = '200'
        and rle.endpoint_end_date = ""
        and rle.resource_end_date = ""
        group by i.field, i.issue_type, rle.resource, rle.pipeline
        order by rle.organisation
        limit 1000 offset {offset}
    """,
    )

    issue_df = get_datasette_query("digital-land", sql)

    return issue_df


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
    dataset_clause = " or ".join(
        ("rle.pipeline = '" + str(dataset) + "'" for dataset in datasets)
    )

    provision_df = get_provisions()

    # Download all endpoint/resources along with their column mappings
    # Use pagination in case rows returned > 1000
    pagination_incomplete = True
    offset = 0
    endpoint_resource_column_field_df_list = []
    while pagination_incomplete:
        endpoint_resource_column_field_df = get_endpoint_resource_column_field_data(
            dataset_clause, offset
        )
        endpoint_resource_column_field_df_list.append(endpoint_resource_column_field_df)
        pagination_incomplete = len(endpoint_resource_column_field_df) == 1000
        offset += 1000
    endpoint_resource_column_field_df = pd.concat(
        endpoint_resource_column_field_df_list
    )

    # Download all issues we need
    pagination_incomplete = True
    offset = 0
    issue_df_list = []
    while pagination_incomplete:
        issue_df = get_endpoint_resource_issue_data(dataset_clause, offset)
        issue_df_list.append(issue_df)
        pagination_incomplete = len(issue_df) == 1000
        offset += 1000
        print(dataset_clause, offset, pagination_incomplete)
    issue_df = pd.concat(issue_df_list)

    # split df into endpoint_resource df and column_field df
    endpoint_resource_df = endpoint_resource_column_field_df.groupby(
        ["organisation", "pipeline"], as_index=False
    ).apply(lambda x: x)
    column_field_df = endpoint_resource_column_field_df[
        ["pipeline", "resource", "column", "field"]
    ].rename(columns={"pipeline": "dataset"})

    # join to provision and bring through cohort and start date
    endpoint_resource_df = endpoint_resource_df.merge(
        provision_df, how="inner", on="organisation"
    )

    # replace licence NaNs with blank
    endpoint_resource_df["licence"].replace(np.nan, "", inplace=True)

    # table of unique resources and pipelines
    resource_df = (
        endpoint_resource_df[["pipeline", "resource"]].drop_duplicates().dropna(axis=0)
    )

    issue_severity_lookup = get_issue_types_by_severity(["error"])

    # get results for col mappings and fields in arrays
    results_col_map = [
        column_field_df[column_field_df["resource"] == r["resource"]]
        for index, r in resource_df.iterrows()
    ]
    results_issues = [
        issue_df[issue_df["resource"] == r["resource"]]
        for index, r in resource_df.iterrows()
    ]

    # concat the results, resources which errored with have NaNs in query results fields
    results_col_map_df = pd.concat(results_col_map)
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
    endpoint_resource_df.rename(columns={"pipeline": "dataset"}, inplace=True)

    # left join from endpoint resource table to all the fields that each dataset should have
    resource_spec_fields_df = endpoint_resource_df[
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
    resource_fields_match = resource_spec_fields_df

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
    # csv_out_cols = [
    #     "organisation",
    #     "name",
    #     "cohort",
    #     "dataset",
    #     "endpoint",
    #     "licence",
    #     "resource",
    #     "status",
    #     "latest_log_entry_date",
    #     "endpoint_entry_date",
    #     "field",
    #     "field_supplied",
    #     "field_matched",
    #     "field_errors",
    #     "field_error_free",
    #     "field_supplied_pct",
    #     "field_error_free_pct",
    #     "field_matched_pct",
    # ]
    print(final_count)

    headers = [
        *map(
            lambda column: {"text": column},
            final_count.columns.values,
        )
    ]
    rows = []
    for index, r in final_count.iterrows():
        row = []
        for cell in r:
            text = cell
            row.append({"text": text})
        rows.append(row)
    return {"headers": headers, "rows": rows}
