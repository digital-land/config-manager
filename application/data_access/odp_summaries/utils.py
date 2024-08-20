import tempfile

import pandas as pd


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
