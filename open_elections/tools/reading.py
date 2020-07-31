from datetime import datetime
import os
import pandas as pd
from typing import List, Tuple, Callable, Union, Iterable, Optional, Any
import re
from open_elections.tools.logging_helper import get_logger


logger = get_logger(__name__)


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
                 row_cleaners: Callable[[dict], dict] = None,
                 excluded_files: List[str] = None):
        self._source_dir = source_dir
        self.state = state
        self.columns = columns
        self.vote_columns = vote_columns
        self.df_transformers = df_transformers
        self.row_cleaners = row_cleaners
        self.excluded_files = excluded_files

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
                 excluded_files: List[str] = None,
                 columns: List[str] = None,
                 vote_columns: List[str] = None,
                 df_transformers: List[Callable[[pd.DataFrame], pd.DataFrame]] = None,
                 row_cleaners: List[Callable[[dict], None]] = None):
        self.excluded_files = excluded_files
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

        return temp.rename(columns={col: col.rstrip().lower() for col in temp.columns})

    def __init__(self,
                 filepath: str,
                 state_metadata: 'StateMetadata',
                 year: int,
                 date: datetime,
                 election: str,
                 is_special: bool,
                 excluded: bool):
        self.filepath = filepath
        self.state_metadata = state_metadata
        self.date = date
        self.election = election
        self.year = year
        self.is_special = is_special
        self.excluded = excluded

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
                                          filepath=self.filepath)

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


VoteFileBuilder = Callable[[int, str, str, 'StateMetadata', bool], 'VoteFile']
TableDataBuilder = Callable[[pd.DataFrame, StateMetadata], List[dict]]


def files_to_table_data(state_metadata: StateMetadata,
                        vote_file_builder: VoteFileBuilder,
                        table_data_builder: TableDataBuilder) -> List[dict]:
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
    raw_voting_data = pd.concat([vote_file_obj.to_enriched_df() for vote_file_obj in vote_file_objs
                                 if not vote_file_obj.excluded])
    table_data = table_data_builder(raw_voting_data, state_metadata)
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
        result = vote_file_builder(year, dirpath, filename, state_metadata, filename in state_metadata.excluded_files)
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


def get_coerce_to_integer(null_cases: List[Any] = None):
    def inner(value: Any) -> Optional[int]:
        if null_cases and value in null_cases:
            return None
        if type(value) == str:
            # Pandas inserts strange string forms of NaN sometimes
            if value == 'nan':
                return None
            # Some are formatted as floats and strings
            if '.' in value:
                try:
                    return int(float(value))
                except ValueError:
                    raise ValueError('{} is not a valid float'.format(value))
            else:
                # numbers such as 1,000
                if ',' in value:
                    value = value.replace(',', '')
                    # Null out if there is nothing left
                    if value == '':
                        return None
                # Null out if empty
                if value == '':
                    return None
                else:
                    return int(value)
        elif pd.isna(value):
            return None
        elif type(value) == int:
            return value
        elif type(value) == float:
            return int(value)
        else:
            return value

    return inner
