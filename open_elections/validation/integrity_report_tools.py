from open_elections.tools import StateMetadata, VoteFile, VoteFileBuilder, build_file_objects
from open_elections.logging import get_logger
from typing import List, Tuple, Optional, Any, Union, Callable, Iterable
import pandas as pd

logger = get_logger(__name__)


class VoteFileIntegrityReport:
    """
    This class performs file integrity checks. It takes a PrecinctFile instance that corresponds to the file to be
    checked, and a StateMetadata instance that holds information on what columns to expect. It then parses the file,
    stores any errors if they occur, and then proceeds to validate the existence of types and columns.
    """
    def __init__(self, vote_file: VoteFile):
        self.vote_file = vote_file

    def check_pre_cleaning(self) -> Tuple[Optional[Exception], Optional[pd.DataFrame]]:
        """
        Runs the check on the VoteFile object this VoteFileIntegrity object has a pointer to. The check consists of
        parsing, and then if an exception is encountered returning the exception, otherwise
        :return:
        """
        df, file_parse_exception = self.parse_file()
        if df is not None:
            enriched_df = self.vote_file.to_enriched_df()
            return None, self._check_helper(enriched_df.to_dict('records'))

        return file_parse_exception, None

    def check_post_cleaning(self,
                            table_data_builder: Callable[[pd.DataFrame, StateMetadata], List[dict]]) -> pd.DataFrame:
        enriched_df = self.vote_file.to_enriched_df()
        return self._check_helper(table_data_builder(enriched_df, self.vote_file.state_metadata))

    def _check_helper(self, data: List[dict]):
        results = []
        for i, dic in enumerate(data):
            for col in self.vote_file.state_metadata.columns:
                if col in self.vote_file.state_metadata.vote_columns and col in dic:
                    type_check = self.is_numeric(dic[col])
                else:
                    type_check = True
                if not type_check:
                    results.append(dict(line_number=i + 1,
                                        column_name=col,
                                        present=col in dic,
                                        type_check=type_check,
                                        value=dic[col]))

        report_df = pd.DataFrame(results).assign(state=self.vote_file.state_metadata.state,
                                                 filename=self.vote_file.filepath)
        return report_df

    @classmethod
    def is_numeric(cls, value: Any) -> bool:
        """
        This checks whether a type can be converted a integer value for the purpose of parsing. It is intentionally
        quite strict as it should flag any data where the data entry has been performed incorrectly.
        :param value:
        :return:
        """
        vt = type(value)
        if vt in (int, float) or pd.isna(value):
            return True
        elif vt == str:
            if str.isnumeric(value) or str.isnumeric(value.replace(',', '')):
                return True
            try:
                float(value)
                return True
            except ValueError:
                return False

        return False

    def parse_file(self) -> Tuple[Optional[pd.DataFrame], Optional[Exception]]:
        try:
            df = pd.read_csv(self.vote_file.filepath)
            return df, None
        except Exception as e:
            return None, e


def check_pre_clean(state_or_states: Union[StateMetadata, List[StateMetadata]],
                    vote_file_builder: VoteFileBuilder,) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
    if type(state_or_states) == list:
        states = state_or_states
    else:
        states = [state_or_states]

    vote_file_reports = build_vote_file_reports(states, vote_file_builder)
    reports = [vote_file_report.check_pre_cleaning() for vote_file_report in vote_file_reports]
    file_parse_reports = [file_parse_report for file_parse_report, _ in reports]
    pre_clean_reports = [pre_clean_report for _, pre_clean_report in reports]
    return pd.DataFrame(file_parse_reports), pd.concat(pre_clean_reports)


def check_post_clean(state_or_states: Union[StateMetadata, List[StateMetadata]],
                     vote_file_builder: VoteFileBuilder,
                     table_data_builder: Callable[[pd.DataFrame], List[dict]]) -> pd.DataFrame:
    if type(state_or_states) == list:
        states = state_or_states
    else:
        states = [state_or_states]

    vote_file_reports = build_vote_file_reports(states, vote_file_builder)
    reports = [vote_file_report.check_post_cleaning(table_data_builder) for vote_file_report in vote_file_reports]
    return pd.concat(reports)


def build_vote_file_reports(states: List[StateMetadata],
                            vote_file_builder: VoteFileBuilder) -> Iterable[VoteFileIntegrityReport]:
    for state_metadata in states:
        for vote_file in build_file_objects(state_metadata, vote_file_builder):
            yield VoteFileIntegrityReport(vote_file)
