from datetime import datetime
import os
import pandas as pd
from typing import List, Tuple, Callable, Union, Iterable
import re
from doltpy.core import Dolt
from doltpy.core.write import import_list
from open_elections.pk_error_tools import parse_and_check_error
import logging
import mysql


logger = logging.getLogger(__name__)


class StateMetadata:
    """
    Stores state metadata such as where the data lives and required format information for parsing data correctly.
    """
    def __init__(self,
                 source_dir: Union[None, str],
                 state: str,
                 columns: List[str],
                 vote_columns: List[str],
                 df_transformers: List[Callable[[pd.DataFrame], pd.DataFrame]] = None,
                 row_cleaners: Callable[[dict], dict] = None):
        self._source_dir = source_dir
        self.state = state
        self.columns = columns
        self.vote_columns = vote_columns
        self.df_transformers = df_transformers
        self.row_cleaners = row_cleaners

    @property
    def source_dir(self):
        return self._source_dir

    def set_source_dir(self, value: str):
        self._source_dir = value


class StateDataFormat:
    """
    This class allows us to specify state specific columns, transformers, and row cleaners. The motivation is that we
    are loading both national and state level datasets. The former have a common set of fields, and the latter have
    their own specific vote tabulation buckets.

    The motivation for allowing us to optionally specify df_transformers and row_cleaners is to let us manipulate
    data in a way specific to the issues that arise in a certain state's data.
    """
    def __init__(self,
                 columns: List[str] = None,
                 vote_columns: List[str] = None,
                 df_transformers: List[Callable[[pd.DataFrame], pd.DataFrame]] = None,
                 row_cleaners: List[Callable[[dict], None]] = None):
        self.columns = columns
        self.vote_columns = vote_columns
        self.df_transformers = df_transformers
        self.row_cleaners = row_cleaners


class VoteFile:
    DATE_POS = 0
    STATE_POS = 1
    ELECTION_POS = 2

    @classmethod
    def clean_column_names(cls, df: pd.DataFrame) -> pd.DataFrame:
        """
        Some columns have erroneous capitalization and trailing spaces, we remove those.
        :param df:
        :return:
        """
        temp = df.copy()

        for column in temp.columns:
            if column.startswith('Unnamed'):
                temp = temp.drop([column], axis=1)

        return temp.rename(columns={col: col.rstrip().lower() for col in temp.columns})

    def __init__(self,
                 filepath: str,
                 state_metadata: 'StateMetadata',
                 year: int,
                 date: datetime,
                 election: str,
                 is_special: bool):
        self.filepath = filepath
        self.state_metadata = state_metadata
        self.date = date
        self.election = election
        self.year = year
        self.is_special = is_special

        if state_metadata.df_transformers:
            self.df_transformers = [self.clean_column_names] + state_metadata.df_transformers
        else:
            self.df_transformers = [self.clean_column_names]

    def to_enriched_df(self) -> pd.DataFrame:
        logger.info('Parsing file {}'.format(self.filepath))
        try:
            df = pd.read_csv(self.filepath)
        except (pd.errors.ParserError, UnicodeDecodeError) as e:
            logger.error(str(e))
            return pd.DataFrame()
        deduplicated = df.drop_duplicates(keep='first')
        # Add some columns that we extracted from the filepath, and the filepath for debugging
        enriched_df = deduplicated.assign(state=self.state_metadata.state.upper(),
                                          year=self.year,
                                          date=self.date,
                                          election=self.election,
                                          special=self.is_special,
                                          filepath=self.state_metadata.source_dir)

        temp = enriched_df.copy()
        if self.df_transformers:
            for transformer in self.df_transformers:
                temp = transformer(temp)

        return temp


class PrecinctFile(VoteFile):
    pass


class CountyFile(VoteFile):
    pass


class OfficeFile(VoteFile):
    pass


VoteFileBuilder = Callable[[int, str, str, 'StateMetadata'], 'VoteFile']
TableDataBuilder = Callable[[pd.DataFrame], List[dict]]


def load_to_dolt(dolt_dir: str,
                 dolt_table: str,
                 dolt_pks: List[str],
                 state_metadata: StateMetadata,
                 vote_file_builder: VoteFileBuilder,
                 table_data_builder: Callable[[pd.DataFrame], List[dict]]):
    """
    Load to the dolt dir/table specified using given columns for primary keys.
    :param dolt_dir:
    :param dolt_table:
    :param dolt_pks:
    :param state_metadata:
    :param vote_file_builder:
    :param table_data_builder:
    :return:
    """
    logger.info('''Loading data for state {}:
                - dolt_dir    : {}
                - dolt_table  : {}
                - dolt_pks    : {}   
            '''.format(state_metadata.state, dolt_dir, dolt_table, dolt_pks))
    repo = Dolt(dolt_dir)
    table_data = files_to_table_data(state_metadata, vote_file_builder, table_data_builder)
    try:
        import_list(repo, dolt_table, table_data, dolt_pks, import_mode='update', chunk_size=100000)
    except mysql.connector.errors.DatabaseError as e:
        if 'duplicate primary key given: ' in e.msg:
            parse_and_check_error(e.msg, table_data)
        else:
            raise e


def files_to_table_data(state_metadata: StateMetadata,
                        vote_file_builder: VoteFileBuilder,
                        table_data_builder: Callable[[pd.DataFrame], List[dict]]):
    """
    Uses state_metadata instance to map a collection of files to VoteFile objects that can be parsed into voting data.
    The vote_file_builder specifies how to map the file paths, combined with metadata, to VoteFile instances. The
    table_data_builder specifies how to take the DataFrame produced by VoteFile instance and turn it into a dict
    that can be written to Dolt.
    :param state_metadata:
    :param vote_file_builder:
    :param table_data_builder:
    :return:
    """
    vote_file_objs = build_file_objects(state_metadata, vote_file_builder)
    raw_voting_data = pd.concat([vote_file_obj.to_enriched_df() for vote_file_obj in vote_file_objs])
    table_data = table_data_builder(raw_voting_data)
    return table_data


def files_to_df(state_metadata: StateMetadata, vote_file_builder: VoteFileBuilder) -> pd.DataFrame:
    """
    Utility function for getting DataFrames for files in a state, useful for debugging.
    :param state_metadata:
    :param vote_file_builder:
    :return:
    """
    vote_file_objs = build_file_objects(state_metadata, vote_file_builder)
    return pd.concat([vote_file_obj.to_enriched_df() for vote_file_obj in vote_file_objs])


def build_file_objects(state_metadata: StateMetadata, vote_file_builder: VoteFileBuilder) -> Iterable[VoteFile]:
    """
    Traverses the base_dir attribute of the StateMetadata object and uses the vote_file_builder to map that to a
    a VoteFile. Implicitly fielders by yielding when instances are not None.
    :param state_metadata:
    :param vote_file_builder:
    :return:
    """
    files = gather_files(state_metadata.source_dir)

    logger.info(
        'Parsing filenames and extracting election metadata to combine with state metadata to build VoteFile instances'
    )
    for year, dirpath, filename in files:
        result = vote_file_builder(year, dirpath, filename, state_metadata)
        if result:
            yield result


def gather_files(base_dir: str) -> List[Tuple[int, str, str]]:
    logger.info('Collecting voting data files from base directory {}'.format(base_dir))
    year_dirs = [(os.path.join(base_dir, subdir), subdir)
                 for subdir in os.listdir(base_dir) if re.match(r'\d\d\d\d', subdir)]

    for year_dir, year in year_dirs:
        for dirpath, _, filenames in os.walk(year_dir):
            for filename in filenames:
                if filename.endswith('csv'):
                    try:
                        yield int(year), dirpath, filename
                    except ValueError:
                        logger.error('Error parsing year: (year, dirpath, filename): ({}, {}, {}'.format(
                            year, dirpath, filename
                        ))
