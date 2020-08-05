from open_elections.tools import StateDataFormat
import pandas as pd


def rename_total_to_votes(df: pd.DataFrame):
    if 'total_votes' in df.columns:
        return df.rename(columns={'total_votes': 'votes'})
    else:
        return df


national_precinct_dataformat = StateDataFormat(
    df_transformers=[rename_total_to_votes]
)
