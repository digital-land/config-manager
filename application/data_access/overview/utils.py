import tempfile

import pandas as pd


def generate_overview_issue_summary_csv(overview_issue_summary):
    # Can convert any Dataframe to csv
    if type(overview_issue_summary) is pd.DataFrame:
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as csvfile:
            overview_issue_summary.to_csv(csvfile, index=False)
            return csvfile.name
    # Else assume overview issue summary structure
    else:
        dfs = []
        for row in overview_issue_summary["rows"]:
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
