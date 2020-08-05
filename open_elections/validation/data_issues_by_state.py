from open_elections.tools.reading import gather_files
from open_elections.tools.logging_helper import get_logger
import pandas as pd
import os
from typing import List, Mapping, Any, Tuple, Optional
import argparse
import sys

logger = get_logger(__name__)


BASE_SCHEMA_DEF = {
    'office': str,
    'district': int,
    'county': str,
    'precinct': str,
    'party': str,
    'candidate': str,
    'votes': int
}

STATE_SCHEMA_DEF_FILENAME = 'column_types.csv'


class DataFileException(Exception):
    def __init__(self, state: str, year: int, path: str):
        self.state = state
        self.year = year
        self.path = path


class FileEncodingException(DataFileException):
    def __init__(self, state: str, year: int, path: str, encoding_exception: Exception):
        super().__init__(state, year, path)
        self.encoding_exception = encoding_exception

    def __str__(self):
        return 'FileEncodingException caused by exception {}'.format(self.encoding_exception)


class FileFormatException(DataFileException):
    def __init__(self, state: str, year: int, path: str, format_exception: Exception):
        super().__init__(state, year, path)
        self.format_exception = format_exception

    def __str__(self):
        return 'FileFormatException caused by exception {}'.format(self.format_exception)


class ColumnMissingException(DataFileException):
    def __init__(self, state: str, year: int, path: str, column_name: str):
        super().__init__(state, year, path)
        self.column_name = column_name

    def __str__(self):
        return 'Column {} missing from file'.format(self.column_name)


class ValueTypeException(DataFileException):
    def __init__(self,
                 state: str,
                 year: int,
                 path: str,
                 column_name: str,
                 column_type: type,
                 values_and_line_numbers: Mapping[int, Any]):
        super().__init__(state, year, path)
        self.column_name = column_name
        self.column_type = column_type
        self.values_and_line_numbers = values_and_line_numbers

    def __str__(self):
        return 'ValueTypeException caused by values for column {} not being of type {}:\n {}'.format(
            self.column_name,
            self.column_type,
            self.values_and_line_numbers
        )


def validate_file(state: str, year: int, path: str, schema_def: Mapping[str, type]) -> List[DataFileException]:
    data, exception = read_file(state, year, path)
    if exception is not None:
        logger.debug('File {} cannot be parsed into a DataFrame'.format(path))
        return [exception]

    exceptions = []
    columns_present = []

    for column_name in schema_def.keys():
        logger.debug('Checking for column {} in file {}'.format(column_name, path))
        if column_name in data.columns:
            columns_present.append(column_name)
        else:
            logger.debug('Column {} missing from file {}'.format(column_name, path))
            exceptions.append(ColumnMissingException(state, year, path, column_name))

    for column_name in columns_present:
        logger.debug('Checking types of column {} in file {}'.format(column_name, path))
        column_type = schema_def[column_name]
        errors = check_types(data[column_name], column_type)
        if errors:
            exceptions.append(ValueTypeException(state, year, path, column_name, column_type, errors))

    return exceptions


def check_types(values: List[Any], column_type: type) -> Mapping[int, Any]:
    errors = {}
    for i, v in enumerate(values):
        try:
            if not pd.isna(v):
                column_type(v)
        except Exception:
            errors[i] = v

    return errors


def read_file(state: str, year: int, path: str) -> Tuple[Optional[pd.DataFrame], Optional[DataFileException]]:
    try:
        data = pd.read_csv(path)
        return data, None
    # handle the additional types of exeception
    except UnicodeDecodeError as e:
        return None, FileEncodingException(state, year, path, e)
    except pd.errors.ParserError as e:
        return None, FileFormatException(state, year, path, e)


def run_checks(base_dir: str, state: str, years: List[int] = None):
    schema_def = get_schema_def(base_dir)
    result = {}
    for year, dirpath, filename in gather_files(base_dir):
        if not years or year in years:
            path = os.path.join(dirpath, filename)
            result[path] = validate_file(state, year, path, schema_def[year])

    return result


def get_schema_def(base_dir: str) -> Mapping[int, Mapping[str, type]]:
    schema_def = get_base_schema_def(base_dir)
    state_schema_file = os.path.join(base_dir, STATE_SCHEMA_DEF_FILENAME)

    if os.path.exists(state_schema_file):
        df = pd.read_csv(state_schema_file)
        schema_def_cols = ['year', 'column', 'type']
        assert all(col in schema_def_cols for col in df.columns), 'schema def file must contain {}'.format(schema_def_cols)
        for record in df.to_dict('records'):
            year, column_name, column_type = record['year'], record['column'], record['type']
            resolved_column_type = _resolve_column_type(column_type)
            schema_def[year][column_name] = resolved_column_type

    return schema_def


def get_base_schema_def(base_dir: str) -> dict:
    years = set(year for year, _, _ in gather_files(base_dir))
    return {year: BASE_SCHEMA_DEF for year in years}


def _resolve_column_type(column_type: str):
    if column_type == 'string':
        return str
    elif column_type == 'integer':
        return int
    else:
        raise ValueError('Type {} is not supported'.format(column_type))


def display_exceptions(exceptions: Mapping[str, List[DataFileException]]):
    for path, exceptions in exceptions.items():
        logger.error('Showing exceptions for file {}'.format(path))
        for exception in exceptions:
            logger.error(exception)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--years', type=str)
    parser.add_argument('--state', type=str, required=True)
    parser.add_argument('--base-dir', type=str, required=True)
    args = parser.parse_args()

    try:
        years = [int(year) for year in args.years.split(',')]
    except ValueError as e:
        logger.error('--years expects a commma separated list of integer values')
        raise e

    assert os.path.exists(args.base_dir), 'The directory passed to --base-dir must exist'
    exceptions = run_checks(args.base_dir, args.state, years)
    display_exceptions(exceptions)
    if exceptions:
        logger.error('Exceptions found, exiting with non-zero error code')
        sys.exit(1)
    else:
        logger.info('Data is clean, exiting')
        sys.exit(0)



if __name__ == '__main__':
    main()
