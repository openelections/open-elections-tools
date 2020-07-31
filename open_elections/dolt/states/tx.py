from open_elections.tools import StateDataFormat
import pandas as pd

INVALID_VOTES_VALUES = ['#REF!', 'REP', 'DEM', 'LIB', 'GRN', 'votes']


def remove_invalid_vote_counts(dic: dict):
    value = dic['votes']
    if value in INVALID_VOTES_VALUES:
        dic['votes'] = None
    elif type(value) == str and '*' in value:
        dic['votes'] = int(value.lstrip().replace('*', ''))


def insert_missing_votes_column(df: pd.DataFrame) -> pd.DataFrame:
    if 'early_voting' in df.columns and 'election_day' in df.columns and 'votes' not in df.columns:
        return df.assign(votes=df['early_voting'] + df['election_day'])


national_precinct_dataformat = StateDataFormat(
    df_transformers=[insert_missing_votes_column],
    row_cleaners=[remove_invalid_vote_counts]
)
