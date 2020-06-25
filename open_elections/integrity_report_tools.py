from open_elections.tools import StateMetadata, VoteFile, VoteFileBuilder, build_file_objects
from typing import List, Tuple, Optional, Any, Union
import pandas as pd
import logging

logger = logging.getLogger(__name__)


class VoteFileIntegrityReport:
    """
    This class performs file integrity checks. It takes a PrecinctFile instance that corresponds to the file to be
    checked, and a StateMetadata instance that holds information on what columns to expect. It then parses the file,
    stores any errors if they occur, and then proceeds to validate the existence of types and columns.
    """
    def __init__(self, vote_file: VoteFile):
        self.vote_file = vote_file

    def run_check(self) -> Tuple[Optional[Exception], Optional[pd.DataFrame]]:
        """
        Runs the check on the VoteFile object this VoteFileIntegrity object has a pointer to. The check consists of
        parsing, and then if an exception is encountered returning the exception, otherwise
        :return:
        """
        df, file_parse_exception = self.parse_file()
        if df is not None:
            enriched_df = self.vote_file.to_enriched_df()
            dicts = enriched_df.to_dict('records')
            results = []
            for i, dic in enumerate(dicts):
                for col in self.vote_file.state_metadata.columns:
                    if col in self.vote_file.state_metadata.vote_columns and col in dic:
                        type_check = self.is_numeric(dic[col])
                    else:
                        type_check = True
                    if not type_check:
                        results.append(dict(line_number=i+1,
                                            column_name=col,
                                            present=col in dic,
                                            type_check=type_check,
                                            value=dic[col]))

            report_df = pd.DataFrame(results).assign(state=self.vote_file.state_metadata.state,
                                                     filename=self.vote_file.filepath)
            return None, report_df

        return file_parse_exception, None

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


def run_report(state_or_states: Union[StateMetadata, List[StateMetadata]],
               vote_file_builder: VoteFileBuilder) -> Tuple[pd.DataFrame, pd.DataFrame]:
    # get the metadata for each state
    if type(state_or_states) == list:
        states = state_or_states
    else:
        states = [state_or_states]

    file_parse_exceptions = []
    df_reports = []
    for state_metadata in states:
        vote_files = build_file_objects(state_metadata, vote_file_builder)
        reports = [VoteFileIntegrityReport(vote_file) for vote_file in vote_files]

        for report in reports:
            exp, df_report = report.run_check()
            if exp:
                file_parse_exceptions.append(dict(filepath=report.vote_file.filepath,
                                                  exception=str(exp)))
            elif df_report is not None:
                df_reports.append(df_report)
            else:
                raise ValueError('Both exp and df_report undefined')

    return pd.DataFrame(file_parse_exceptions), pd.concat(df_reports)

