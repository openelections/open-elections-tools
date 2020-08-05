from open_elections.tools import StateDataFormat
import pandas as pd


def remove_invalid_vote_counts(dic: dict):
    value = dic['votes']
    if type(value) == str and value in ('-', ' JR."'):
        dic['votes'] = None


def rename_total_to_votes(df: pd.DataFrame):
    if 'total' in df.columns:
        return df.rename(columns={'total': 'votes'})
    else:
        return df


national_precinct_dataformat = StateDataFormat(
    df_transformers=[rename_total_to_votes],
    row_cleaners=[remove_invalid_vote_counts]
)
