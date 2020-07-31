from open_elections.tools import StateDataFormat
import pandas as pd


# need to exclude 20021105__wv__general__monongalia__precinct.csv
def remove_invalid_vote_counts(dic: dict):
    value = dic['votes']
    if type(value) == str and value == ' ':
        dic['votes'] = None


def rename_total_to_votes(df: pd.DataFrame):
    if 'total votes' in df.columns:
        return df.rename(columns={'total votes': 'votes'})
    else:
        return df


national_precinct_dataformat = StateDataFormat(
    excluded_files=['20021105__wv__general__monongalia__precinct.csv'],
    df_transformers=[rename_total_to_votes],
    row_cleaners=[remove_invalid_vote_counts]
)
