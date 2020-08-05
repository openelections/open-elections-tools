from open_elections.tools import StateDataFormat, get_coerce_to_integer
import pandas as pd


def fix_vote_counts(df: pd.DataFrame) -> pd.DataFrame:
    if 'total' in df.columns:
        return df.rename(columns={'total': 'votes'})
    else:
        breakout_cols = [col for col in ['poll', 'edr', 'abs'] if col in df.columns]
        if breakout_cols:
            temp = df.copy()
            for col in breakout_cols:
                if col in temp.columns:
                    temp[col] = temp[col].apply(get_coerce_to_integer([' -   ']))

            return temp.assign(votes=df[breakout_cols].sum(axis=1))
        else:
            return df


national_precinct_dataformat = StateDataFormat(
    df_transformers=[fix_vote_counts]
)

