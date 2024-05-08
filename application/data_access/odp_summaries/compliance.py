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
        ) cf on cf.resource = rle.resource and rle.pipeline = cf.dataset
        where ({dataset_clause})
        and rle.status = '200'
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
        where ({dataset_clause})
        and rle.status = '200'
        and rle.endpoint_end_date = ""
        and rle.resource_end_date = ""
        group by i.field, i.issue_type, rle.resource, rle.pipeline
        order by rle.organisation
        limit 1000 offset {offset}
    """,
    )

    issue_df = get_datasette_query("digital-land", sql)

    return issue_df


def get_provisions(cohort_clause):
    sql = f"""
    SELECT
        p.cohort,
        p.notes,
        p.organisation,
        p.project,
        p.provision_reason,
        c.start_date as cohort_start_date
    FROM
        provision p
    INNER JOIN
        cohort c on c.cohort = p.cohort
    WHERE
        p.provision_reason = "expected"
    AND p.project == "open-digital-planning"
    {cohort_clause}
    GROUP BY
        p.organisation
    ORDER BY
        cohort_start_date
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
    params = {
        "cohorts": COHORTS,
        "dataset_types": DATASET_TYPES,
        "selected_dataset_types": dataset_types,
        "selected_cohorts": cohorts,
    }
    filtered_cohorts = [
        x for x in cohorts if cohorts[0] in [cohort["id"] for cohort in COHORTS]
    ]
    cohort_clause = (
        "and " + " or ".join(("p.cohort = '" + str(n) + "'" for n in filtered_cohorts))
        if filtered_cohorts
        else ""
    )
    if dataset_types == ["spatial"]:
        datasets = SPATIAL_DATASETS
    elif dataset_types == ["document"]:
        datasets = DOCUMENT_DATASETS
    else:
        datasets = ALL_DATASETS
    dataset_clause = " or ".join(
        ("rle.pipeline = '" + str(dataset) + "'" for dataset in datasets)
    )

    provision_df = get_provisions(cohort_clause)

    # Download all endpoint/resources combined with their column mappings (to minimise no. of requests)
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
    if len(endpoint_resource_column_field_df_list) == 0:
        return {"params": params, "rows": [], "headers": []}
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
    issue_df = pd.concat(issue_df_list)

    # split combined df into endpoint_resource df and column_field df
    # use drop_duplicates because pandas groupby apparently can't group correctly
    endpoint_resource_df = endpoint_resource_column_field_df.drop_duplicates(
        ["pipeline", "organisation"]
    )
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
        column_field_df[
            (column_field_df["resource"] == r["resource"])
            & (column_field_df["dataset"] == r["pipeline"])
        ]
        for index, r in resource_df.iterrows()
    ]
    results_issues = [
        issue_df[issue_df["resource"] == r["resource"]]
        for index, r in resource_df.iterrows()
    ]
    # Exit early if no results
    if len(results_col_map) == 0 or len(results_issues) == 0:
        return {"params": params, "rows": [], "headers": []}
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
            "cohort_start_date",
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
                "cohort_start_date",
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
        "organisation",
        "name",
        "cohort",
        "dataset",
        "licence",
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
                "classes": "reporting-table-cell",
                "attributes": {
                    "style": f"background:rgba(0, 112, 60, {round(float(cell),2) if type(cell) is float else 0});",
                    "unsafe-inline": "",
                },
            }
            for cell in r
        ]
        for index, r in final_count[out_cols].iterrows()
    ]

    return {"headers": headers, "rows": rows, "params": params}, final_count[
        csv_out_cols
    ]


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
