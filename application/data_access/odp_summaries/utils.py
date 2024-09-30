import tempfile

import pandas as pd

from application.data_access.datasette_utils import get_datasette_query


def generate_odp_summary_csv(odp_summary):
    # Can convert any Dataframe to csv
    if type(odp_summary) is pd.DataFrame:
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as csvfile:
            odp_summary.to_csv(csvfile, index=False)
            return csvfile.name
    # Else assume odp summary structure
    else:
        dfs = []
        for row in odp_summary["rows"]:
            for cell in row:
                try:
                    data = cell["data"]
                    dfs.append(pd.DataFrame.from_records(data))
                except Exception:
                    pass
        output_df = pd.concat(dfs)
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
        "AND " + " or ".join(("c.cohort = '" + str(n) + "'" for n in filtered_cohorts))
        if filtered_cohorts
        else ""
    )
    sql = f"""
    SELECT
        p.cohort,
        p.organisation,
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
        cohort_start_date,
        p.cohort
    """
    provision_df = get_datasette_query("digital-land", sql)
    return provision_df
