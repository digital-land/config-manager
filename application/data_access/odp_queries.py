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

COHORTS = [
    "ODP-Track1",
    "ODP-Track2",
    "ODP-Track3",
    "ODP-Track4",
    "RIPA-BOPS",
]


def get_odp_status_summary(dataset_types, cohorts):
    filtered_cohorts = [x for x in cohorts if cohorts[0] in COHORTS]
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
            select p.organisation, p.cohort, o.name from provision p
                inner join organisation o on o.organisation = p.organisation
                where "cohort" not like "RIPA-Beta" and "project" like "open-digital-planning"
            group by p.organisation
        )
        as odp_orgs
        left join reporting_latest_endpoints rle on replace(rle.organisation, '-eng', '') = odp_orgs.organisation
        {cohort_clause}
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
        datasets = []
        if "spatial" in dataset_types:
            datasets += SPATIAL_DATASETS
        if "document" in dataset_types:
            datasets += DOCUMENT_DATASETS
        if datasets == []:
            datasets = SPATIAL_DATASETS + DOCUMENT_DATASETS
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
        average_percentage = str(int(100 * (percentages / len(rows))))[:2] + "%"
        datasets_added = str(datasets_added)
        max_datasets = len(rows) * len(datasets)

        headers = [
            {"text": "Cohort"},
            {"text": "Organisation"},
            *map(lambda dataset: {"text": dataset}, datasets),
            {"text": "% provided"},
        ]
        return {
            "rows": rows,
            "headers": headers,
            "percentage_datasets_added": average_percentage,
            "datasets_added": datasets_added,
            "max_datasets": max_datasets,
        }

    else:
        return None


def create_row(organisation, cohort, name, status_df, datasets):
    row = []
    row.append({"text": cohort, "classes": "reporting-table-sticky-cell"})
    row.append({"text": name, "classes": "reporting-table-sticky-cell"})
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


def generate_csv(odp_summary):
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
