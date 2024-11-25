import tempfile

import pandas as pd

from application.data_access.datasette_utils import get_datasette_query


def generate_odp_summary_csv(odp_summary):
    dataset_pipelines = {
        "article-4-direction": ["article-4-direction", "article-4-direction-area"],
        "conservation-area": ["conservation-area", "conservation-area-document"],
        "listed-building": ["listed-building-outline"],
        "tree-preservation-order": [
            "tree-preservation-order",
            "tree-preservation-zone",
            "tree",
        ],
    }
    organisation_data = get_organisation_code()
    org_lookup = dict(zip(organisation_data["name"], organisation_data["organisation"]))

    # Can convert any Dataframe to csv
    if type(odp_summary) is pd.DataFrame:
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as csvfile:
            odp_summary.to_csv(csvfile, index=False)
            return csvfile.name
    # Else assume odp summary structure
    else:
        dfs = []
        for row in odp_summary["rows"]:
            cohort = row[0]["text"]
            name = row[1]["text"]
            data_found = False

            for cell in row:
                try:
                    data = cell.get("data", [])
                    if data:
                        df = pd.DataFrame.from_records(data)
                        dfs.append(df)
                        data_found = True
                except Exception:
                    pass

            # If no data found, placeholder rows
            if not data_found:
                placeholder_rows = []
                for collection, pipelines in dataset_pipelines.items():
                    for pipeline in pipelines:
                        placeholder_row = {
                            "organisation": org_lookup.get(name, "LPA code not found"),
                            "cohort": cohort,
                            "name": name,
                            "collection": collection,
                            "pipeline": pipeline,
                            "endpoint": "No endpoint added",
                            "endpoint_url": "",
                            "licence": "",
                            "status": "",
                            "days_since_200": "",
                            "exception": "",
                            "resource": "",
                            "latest_log_entry_date": "",
                            "endpoint_entry_date": "",
                            "endpoint_end_date": "",
                            "resource_start_date": "",
                            "resource_end_date": "",
                            "cohort_start_date": "",
                        }
                        placeholder_rows.append(placeholder_row)

                dfs.append(pd.DataFrame(placeholder_rows))

        if dfs:
            output_df = pd.concat(dfs, ignore_index=True)

            columns_order = ["organisation", "cohort", "name"] + [
                col
                for col in output_df.columns
                if col not in ["organisation", "cohort", "name"]
            ]
            output_df = output_df[columns_order]

            with tempfile.NamedTemporaryFile(mode="w", delete=False) as csvfile:
                output_df.to_csv(csvfile, index=False)
                return csvfile.name


def get_provisions(selected_cohorts, all_cohorts):
    filtered_cohorts = [
        x
        for x in selected_cohorts
        if selected_cohorts[0] in [cohort["id"] for cohort in all_cohorts]
    ]
    cohort_clause = (
        "AND ("
        + " or ".join("c.cohort = '" + str(n) + "'" for n in filtered_cohorts)
        + ")"
        if filtered_cohorts
        else ""
    )
    sql = f"""
    SELECT
        p.cohort,
        p.organisation,
        c.start_date as cohort_start_date,
        org.name as name
    FROM
        provision p
    INNER JOIN
        cohort c on c.cohort = p.cohort
    JOIN organisation org
    WHERE
        p.provision_reason = "expected"
    AND p.project == "open-digital-planning"
    {cohort_clause}
    AND org.organisation == p.organisation
    GROUP BY
        p.organisation
    ORDER BY
        cohort_start_date,
        p.cohort
    """
    provision_df = get_datasette_query("digital-land", sql)
    return provision_df


def get_organisation_code():
    sql = """
select
  organisation,
  name
from
  organisation
    """
    code_data = get_datasette_query("digital-land", sql)
    return code_data
