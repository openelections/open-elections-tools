from open_elections.tools import StateDataFormat
import pandas as pd


def remove_invalid_vote_counts(dic: dict):
    value = dic['votes']
    if type(value) == str and value in ('***', '(< 25)'):
        dic['votes'] = None


def rename_votes_col(df: pd.DataFrame) -> pd.DataFrame:
    if 'total_votes' in df.columns:
        return df.rename(columns={'total_votes': 'votes'})
    else:
        return df


national_precinct_dataformat = StateDataFormat(
    df_transformers=[rename_votes_col],
    row_cleaners=[remove_invalid_vote_counts]
)
