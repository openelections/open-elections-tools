from open_elections.tools import StateDataFormat
import pandas as pd


def remove_invalid_vote_counts(dic: dict):
    value = dic['votes']
    if value in ['Write-ins', 'ESTES R', 'I']:
        dic['votes'] = None


def rename_vote_cols(df: pd.DataFrame) -> pd.DataFrame:
    if 'Unnamed: 6'.lower() in df.columns:
        return df.rename(columns={'Unnamed: 6'.lower(): 'votes'})
    else:
        return df


national_precinct_dataformat = StateDataFormat(
    df_transformers=[rename_vote_cols],
    row_cleaners=[remove_invalid_vote_counts]
)
