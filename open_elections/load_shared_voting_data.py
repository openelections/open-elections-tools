from open_elections.tools import PrecinctFile, StateMetadata, load_to_dolt
from open_elections.config import build_state_metadata
from open_elections.integrity_report_tools import run_report
import os
from typing import List, Union
import pandas as pd
from datetime import datetime
import logging
import sys
import argparse


logger = logging.getLogger(__name__)


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
                              state_metadata: StateMetadata) -> Union[None, PrecinctFile]:
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

    return PrecinctFile(os.path.join(path, file_name), state_metadata, year, date, election, special)


# TODO
#   we actually just want to run a series of row cleaners that exist per state
def extract_precinct_voting_data(raw_precinct_data: pd.DataFrame) -> List[dict]:
    not_null_pk = ensure_pks_non_null(raw_precinct_data[VOTING_DATA_PKS + ['votes']])
    deduplicated = not_null_pk.drop_duplicates(subset=VOTING_DATA_PKS)
    logger.warning(
        'There are {} records in the raw precinct data, and {} after de-duplicating'.format(
            len(not_null_pk),
            len(deduplicated)
        )
    )

    dicts = deduplicated.to_dict('records')

    result = []
    for dic in dicts:
        for key, value in dic.items():
            if key == 'votes':
                if type(value) == str:
                    # A bunch of cases of corrupt voting data we set to null
                    if 'X' in value:
                        value = value.lstrip('X')
                    if value.endswith('S'):
                        value = value.rstrip('S')
                    if value in ('"X"',
                                 'X',
                                 '-',
                                 '',
                                 'S',
                                 'x',
                                 ' ',
                                 'X ',
                                 '(< 25)',
                                 'i',
                                 'None',
                                 'Write-ins',
                                 'ESTES R',
                                 'I',
                                 '.',
                                 ':',
                                 '`',
                                 '1 1',
                                 'Ballots Cast',
                                 'votes',
                                 'Total'):
                        dic[key] = None
                    else:
                        # Some are formatted as floats and strings
                        if '.' in value:
                            dic[key] = int(float(value))
                        else:
                            # Some integers have random characters in them, remove them
                            if ',' in value or '*' in value:
                                value = value.replace(',', '').replace('*', '')
                                # Null out if there is nothing left
                                if value == '':
                                    dic[key] = None
                            # Null out if empty
                            if value == '':
                                dic[key] = None
                            else:
                                # Finally we (might) have an integer
                                dic[key] = int(value)
                elif pd.isna(value):
                    dic[key] = None
                elif type(value) == int:
                    dic[key] = value
                elif type(value) == float:
                    dic[key] = int(value)
            else:
                pass

        result.append(dic)

    return result


def ensure_pks_non_null(raw_data: pd.DataFrame) -> pd.DataFrame:
    temp = raw_data.copy()
    for col in VOTING_DATA_PKS:
        if col in temp.columns:
            temp[col] = temp[col].fillna(DEFAULT_PK_VALUE)
        else:
            temp[col] = 'NA'

    return temp


def clean_vote_col_names(precinct_df: pd.DataFrame) -> pd.DataFrame:
    return precinct_df.rename(columns={'vote': 'votes'}) if 'vote' in precinct_df.columns else precinct_df


def run_integrity_report(state_or_states: Union[str, List[str]]):
    if type(state_or_states) == list:
        states = state_or_states
    else:
        states = [state_or_states]

    state_metadata_list = [build_metadata_helper(state) for state in states]

    logger.info('Building state metadata')
    parsing_reports, data_reports = [], []
    for state_metadata in state_metadata_list:
        parsing_report, data_report = run_report(state_metadata, filepath_to_precinct_file)
        parsing_reports.append(parsing_report)
        data_reports.append(data_report)

    return pd.concat(parsing_reports), pd.concat(data_reports)


def build_metadata_helper(state) -> StateMetadata:
    return build_state_metadata(state,
                                STATE_DATA_FORMAT_MEMBER,
                                False,
                                columns=VOTING_DATA_PKS + ['votes'],
                                vote_columns=['votes'],
                                df_transformers=[clean_vote_col_names, ensure_pks_non_null])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--state', type=str, help='State to load data for', required=True)
    parser.add_argument('--integrtiy-report', action='store_true')
    parser.add_argument('--load-data', )
    parser.add_argument('--dolt-dir', type=str, help='Dolt repo directory')
    args = parser.parse_args()

    state_metadata = build_metadata_helper(args.state)

    logger.setLevel('INFO')
    logger.addHandler(logging.StreamHandler(sys.stdout))

    load_to_dolt(args.dolt_dir,
                 'national_voting_data',
                 VOTING_DATA_PKS,
                 state_metadata,
                 filepath_to_precinct_file,
                 extract_precinct_voting_data)


if __name__ == '__main__':
    main()
