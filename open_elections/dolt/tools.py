from doltpy.core import Dolt
from doltpy.core.write import import_list
from typing import List
from open_elections.tools.reading import StateMetadata, VoteFileBuilder, TableDataBuilder, files_to_table_data
from open_elections.tools.logging_helper import get_logger

logger = get_logger(__name__)


def load_to_dolt(repo: Dolt,
                 dolt_table: str,
                 dolt_pks: List[str],
                 state_metadata: StateMetadata,
                 vote_file_builder: VoteFileBuilder,
                 table_data_builder: TableDataBuilder):
    """
    Load to the dolt dir/table specified using given columns for primary keys.
    :param repo:
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
            '''.format(state_metadata.state, repo.repo_dir(), dolt_table, dolt_pks))
    table_data = files_to_table_data(state_metadata, vote_file_builder, table_data_builder)
    import_list(repo, dolt_table, table_data, dolt_pks, import_mode='update', batch_size=100000)
