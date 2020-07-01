from open_elections.tools import StateDataFormat
import pandas as pd


INVALID_VOTE_VALUES = ['S', 'Michael L Conroy', 'Kenneth P La Valle', 'Blank', 'Void',
       'Scattering', 'John J Flanagan', 'Joseph Lombardi',
       'Thomas D Croci', 'Adrienne Esposito', 'John R Alberts',
       'Philip M Boyle', 'Georgina Bowman', 'Bruce P Kennedy Jr',
       'Carl L Marcellino', 'David W Denenberg', 'Michael Venditto',
       'Bridget M Fleming', 'Kenneth P Lavalle', 'Errol D Toulon Jr',
       'Francis T Genco', 'Lee M Zeldin', 'Rick Montano',
       'David B Wright', 'Charles J Fuschillo Jr', 'Carol A Gordon']


def remove_invalid_vote_counts(dic: dict):
    value = dic['votes']
    if value in INVALID_VOTE_VALUES:
        dic['votes'] = None


def fix_vote_counts(df: pd.DataFrame) -> pd.DataFrame:
    if 'election_day' in df.columns and 'absentee' in df.columns and 'votes' not in df.columns:
        return df.assign(votes=df['election_day'] + df['absentee'])
    else:
        return df


national_precinct_dataformat = StateDataFormat(
    df_transformers=[fix_vote_counts],
    row_cleaners=[remove_invalid_vote_counts]
)
