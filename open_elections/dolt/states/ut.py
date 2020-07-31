from open_elections.tools import StateDataFormat
import pandas as pd


def remove_invalid_vote_counts(dic: dict):
    value = dic['votes']
    if type(value) == str and value in ('*', '-'):
        dic['votes'] = None


def rename_total_to_votes(df: pd.DataFrame):
    if 'total votes' in df.columns:
        return df.rename(columns={'total votes': 'votes'})
    else:
        return df


def fix_misnamed_cols(df: pd.DataFrame) -> pd.DataFrame:
    if 'attribute' in df.columns and 'value' in df.columns:
        return df.rename(columns={'attribute': 'candidate', 'value': 'votes'})
    else:
        return df


national_precinct_dataformat = StateDataFormat(
    df_transformers=[rename_total_to_votes, fix_misnamed_cols],
    row_cleaners=[remove_invalid_vote_counts]
)
