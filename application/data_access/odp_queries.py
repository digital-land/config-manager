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


def get_odp_status_summary():
    sql = """
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
    """
    status_df = get_datasette_query("digital-land", sql)
    spatial_rows = []
    document_rows = []
    both_rows = []
    if status_df is not None:
        organisation_cohort_dict_list = (
            status_df[["organisation", "cohort", "name"]]
            .drop_duplicates()
            .to_dict(orient="records")
        )
        for organisation_cohort_dict in organisation_cohort_dict_list:
            organisation = organisation_cohort_dict["organisation"]
            spatial_row = []
            spatial_row.append({"text": organisation_cohort_dict["cohort"]})
            spatial_row.append({"text": organisation_cohort_dict["name"]})
            for dataset in SPATIAL_DATASETS:
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
                    spatial_row.append({"text": "Ok", "classes": "green-background"})
                elif (
                    status != "None"
                    and status != "200"
                    and df_row["endpoint"].values[0] != ""
                ):
                    spatial_row.append({"text": "Bad", "classes": "red-background"})
                else:
                    spatial_row.append(
                        {"text": "No endpoint", "classes": "yellow-background"}
                    )
            spatial_rows.append(spatial_row)
            document_row = []
            document_row.append({"text": organisation_cohort_dict["cohort"]})
            document_row.append({"text": organisation})
            for dataset in DOCUMENT_DATASETS:
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
                    document_row.append({"text": "Ok", "classes": "green-background"})
                elif (
                    status != "None"
                    and status != "200"
                    and df_row["endpoint"].values[0] != ""
                ):
                    document_row.append({"text": "Bad", "classes": "red-background"})
                else:
                    document_row.append(
                        {"text": "No endpoint", "classes": "yellow-background"}
                    )
            document_rows.append(document_row)
            both_rows.append([*spatial_row, *document_row[2:]])

        base_headers = [{"text": "Cohort"}, {"text": "Organisation"}]
        spatial_headers = [*map(lambda dataset: {"text": dataset}, SPATIAL_DATASETS)]
        document_headers = [*map(lambda dataset: {"text": dataset}, DOCUMENT_DATASETS)]
        both_headers = [*spatial_headers, *document_headers]
        return {
            "spatial_rows": spatial_rows,
            "document_rows": document_rows,
            "both_rows": both_rows,
            "spatial_headers": [
                *base_headers,
                *spatial_headers,
            ],
            "document_headers": [
                *base_headers,
                *document_headers,
            ],
            "both_headers": [
                *base_headers,
                *both_headers,
            ],
        }

    else:
        return None
