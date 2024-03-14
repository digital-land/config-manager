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

COHORTS = [
    "ODP-Track1",
    "ODP-Track2",
    "ODP-Track3",
    "ODP-Track4",
    "RIPA-BOPS",
]


def get_odp_status_summary(dataset_type, cohort):
    cohort_filter = f"where odp_orgs.cohort = '{cohort}'" if cohort in COHORTS else ""
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
            select p.organisation, p.cohort, o.name from provision p
                inner join organisation o on o.organisation = p.organisation
                where "cohort" not like "RIPA-Beta" and "project" like "open-digital-planning"
            group by p.organisation
        )
        as odp_orgs
        left join reporting_latest_endpoints rle on replace(rle.organisation, '-eng', '') = odp_orgs.organisation
        {cohort_filter}
        order by odp_orgs.cohort
    """
    status_df = get_datasette_query("digital-land", sql)
    rows = []
    if status_df is not None:
        organisation_cohort_dict_list = (
            status_df[["organisation", "cohort", "name"]]
            .drop_duplicates()
            .to_dict(orient="records")
        )
        if dataset_type == "spatial":
            datasets = SPATIAL_DATASETS
        elif dataset_type == "document":
            datasets = DOCUMENT_DATASETS
        else:
            datasets = [*SPATIAL_DATASETS, *DOCUMENT_DATASETS]
        for organisation_cohort_dict in organisation_cohort_dict_list:
            rows.append(
                create_row(
                    organisation_cohort_dict["organisation"],
                    organisation_cohort_dict["cohort"],
                    status_df,
                    datasets,
                )
            )

        headers = [
            {"text": "Cohort"},
            {"text": "Organisation"},
            *map(lambda dataset: {"text": dataset}, datasets),
        ]
        return {"rows": rows, "headers": headers}

    else:
        return None


def create_row(organisation, cohort, status_df, datasets):
    row = []
    row.append({"text": cohort})
    row.append({"text": organisation})
    for dataset in datasets:
        df_row = status_df[
            (status_df["organisation"] == organisation)
            & (status_df["pipeline"] == dataset)
        ]
        if len(df_row) != 0:
            if df_row["status"].values:
                status = df_row["status"].values[0]
            else:
                # Look at exception for status
                if df_row["status"].values:
                    status = df_row["status"].values[0]
        else:
            status = "None"

        if status == "200":
            row.append(
                {
                    "text": "Yes",
                    "classes": "reporting-good-background",
                    "data": df_row.fillna("").to_dict(orient="records")
                    if (len(df_row) != 0)
                    else {},
                }
            )
        elif (
            status != "None" and status != "200" and df_row["endpoint"].values[0] != ""
        ):
            row.append(
                {
                    "text": "Yes - erroring",
                    "classes": "reporting-bad-background",
                    "data": df_row.fillna("").to_dict(orient="records")
                    if (len(df_row) != 0)
                    else {},
                }
            )
        else:
            row.append(
                {
                    "text": "No endpoint",
                    "classes": "reporting-null-background",
                    "data": df_row.fillna("").to_dict(orient="records")
                    if (len(df_row) != 0)
                    else {},
                }
            )
    return row
