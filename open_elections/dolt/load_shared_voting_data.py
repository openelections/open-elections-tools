from open_elections.tools.reading import PrecinctFile, StateMetadata, get_coerce_to_integer
from open_elections.tools.config import build_state_metadata
from open_elections.validation.integrity_report_tools import check_post_clean, check_pre_clean
from open_elections.tools.logging_helper import get_logger
from open_elections.dolt.tools import load_to_dolt
from doltpy.core import Dolt
import os
from typing import List, Union, Any
import pandas as pd
from datetime import datetime
import argparse
import re


logger = get_logger(__name__)


VOTING_DATA_PKS = [
    'state',
    'year',
    'date',
    'election',
    'special',
    'office',
    'district',
    'county',
    'precinct',
    'party',
    'candidate',
]

DEFAULT_PK_VALUE = 'NA'

STATE_DATA_FORMAT_MEMBER = 'national_precinct_dataformat'


def filepath_to_precinct_file(year: int,
                              path: str,
                              file_name: str,
                              state_metadata: StateMetadata,
                              excluded: bool) -> Union[None, PrecinctFile]:
    split = file_name.split('.')[0].split('__')

    special = 'special' in split
    if special:
        split.remove('special')

    if 'precinct' not in split and 'ward' not in split:
        logger.warning(
            'Passed non-precinct file to precinct vote file builder, ignoring: '.format(os.path.join(path, file_name))
        )
        return None

    # Deals with the case of files formatted like:
    #   20160913__ny__republican__primary__richmond__precinct.csv
    # since it will have election type 'primary' and party is populated, this is redundant
    if 'primary' in split:
        if 'republican' in split:
            split.remove('republican')
        if 'democrat' in split:
            split.remove('democrat')

    state = split[PrecinctFile.STATE_POS]
    date_str = split[PrecinctFile.DATE_POS]
    election = split[PrecinctFile.ELECTION_POS]

    assert state.lower() == state_metadata.state.lower(), 'Extracted state and state_metadata.state must be the same'
    
    if date_str.startswith('_'):
        date = datetime.strptime(date_str.lstrip('_'), '%Y%m%d')
    else:
        date = datetime.strptime(date_str, '%Y%m%d')

    return PrecinctFile(os.path.join(path, file_name), state_metadata, year, date, election, special, excluded)


# TODO
#   we actually just want to run a series of row cleaners that exist per state
def extract_precinct_voting_data(raw_precinct_data: pd.DataFrame, state_metadata: StateMetadata) -> List[dict]:
    clean_precincts = raw_precinct_data.assign(precinct=raw_precinct_data['precinct'].apply(coerce_to_string),
                                               district=raw_precinct_data['district'].apply(coerce_to_string))
    not_null_pk = ensure_pks_non_null(clean_precincts[VOTING_DATA_PKS + ['votes']])
    deduplicated = not_null_pk.drop_duplicates(subset=VOTING_DATA_PKS)
    logger.warning(
        'There are {} records in the raw precinct data, and {} after de-duplicating'.format(
            len(not_null_pk),
            len(deduplicated)
        )
    )

    dicts = deduplicated.to_dict('records')

    result = []
    # We want to run coerce_votes_numeric last since it throws an exception on invalid data.
    for dic in dicts:
        for row_cleaner in state_metadata.row_cleaners:
            row_cleaner(dic)

        result.append(dic)

    return result


def coerce_votes_numeric(dic: dict):
    """
    This function takes care of some common Panda Poo that gets stuck to the data, and also handles coercing valid
    numerics to an integer object in Python memory. It also handles common strings that we infer to mean null/zero, such
    as the empty string.
    """
    dic['votes'] = get_coerce_to_integer()(dic['votes'])


def ensure_pks_non_null(raw_data: pd.DataFrame) -> pd.DataFrame:
    temp = raw_data.copy()
    for col in VOTING_DATA_PKS:
        if col in temp.columns:
            temp[col] = temp[col].fillna(DEFAULT_PK_VALUE)
        else:
            temp[col] = 'NA'

    return temp


def coerce_to_string(value: Any):
    if type(value) == int:
        return str(value)
    elif type(value) == float:
        return str(int(value))
    elif type(value) == str:
        if re.match(r'\d+\.\d+$', value):
            return str(int(float(value)))
        return value
    else:
        raise ValueError('Invalid preinct value {}'.format(value))


def clean_vote_col_names(precinct_df: pd.DataFrame) -> pd.DataFrame:
    return precinct_df.rename(columns={'vote': 'votes'}) if 'vote' in precinct_df.columns else precinct_df


def pre_clean_integrity_report(state_or_states: Union[str, List[str]]):
    if type(state_or_states) == list:
        states = state_or_states
    else:
        states = [state_or_states]

    state_metadata_list = [build_metadata_helper(state) for state in states]

    logger.info('Building state metadata')
    return check_pre_clean(state_metadata_list, filepath_to_precinct_file)


def post_clean_integrity_report(state_or_states: Union[str, List[str]]):
    if type(state_or_states) == list:
        states = state_or_states
    else:
        states = [state_or_states]

    state_metadata_list = [build_metadata_helper(state) for state in states]
    return check_post_clean(state_metadata_list, filepath_to_precinct_file, extract_precinct_voting_data)


def build_metadata_helper(state: str) -> StateMetadata:
    return build_state_metadata(state,
                                STATE_DATA_FORMAT_MEMBER,
                                False,
                                columns=VOTING_DATA_PKS + ['votes'],
                                vote_columns=['votes'],
                                df_transformers=[clean_vote_col_names, ensure_pks_non_null],
                                row_cleaners=[coerce_votes_numeric])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--state', type=str, help='State to load data for', required=True)
    parser.add_argument('--load-data', )
    parser.add_argument('--dolt-dir', type=str, help='Dolt repo directory')
    parser.add_argument('--start-dolt-server', action='store_true')
    args = parser.parse_args()

    repo = Dolt(args.dolt_dir)
    if args.start_dolt_server:
        logger.info('start-dolt-server detected, starting server sub process')
        repo.sql_server(loglevel='trace')

    state_metadata = build_metadata_helper(args.state)

    load_to_dolt(repo,
                 'national_voting_data',
                 VOTING_DATA_PKS,
                 state_metadata,
                 filepath_to_precinct_file,
                 extract_precinct_voting_data)


if __name__ == '__main__':
    main()
