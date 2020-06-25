import os
from open_elections.tools import StateMetadata, StateDataFormat
import pandas as pd
from typing import List, Callable, Optional
import importlib


BASE_DIR = '/Users/oscarbatori/Documents/open-elections'


def build_state_metadata(state: str,
                         state_module_member: str = None,
                         strict: bool = True,
                         columns: List[str] = None,
                         vote_columns: List[str] = None,
                         df_transformers: List[Callable[[pd.DataFrame], pd.DataFrame]] = None,
                         row_cleaners: List[Callable[[dict], None]] = None) -> StateMetadata:
    """
    This is a factor method for state metadata that allows for a number of ways ot spcify state specific attributes:
        - they can be explicitly specified (for example in nationwide voting data we want to the same set of columns)
        - they can loaded from a module where we look for the variable named

    :param state:
    :param state_module_member: the member to try and retrieve from the state module of type StateDataFormat
    :param strict: indicates whether to fail in the case of the module not being present
    :param columns:
    :param vote_columns:
    :param df_transformers:
    :param row_cleaners:
    :return:
    """
    assert state in STATES, 'State {} not in: {}'.format(state, STATES)

    # try_module is True and so we should try and grab attributes from the state
    try:
        module = importlib.import_module(get_state_module_path(state))
        if hasattr(module, state_module_member):
            state_data_format = getattr(module, state_module_member)
            if type(state_data_format) != StateDataFormat:
                raise TypeError('state_module_member must resolve an object of type StateDataFormat')
            else:
                columns = _combine_helper(columns, state_data_format.columns)
                vote_columns = _combine_helper(vote_columns, state_data_format.vote_columns)
                df_transformers = _combine_helper(df_transformers, state_data_format.df_transformers)
                row_cleaners = _combine_helper(row_cleaners, state_data_format.row_cleaners)
        else:
            if strict:
                raise NotImplementedError('No member {} in module {}'.format(state_module_member,
                                                                             get_state_module_path(state)))
    except ModuleNotFoundError:
        if strict:
            raise ModuleNotFoundError('You specified state_module_member {}, no such module found'.format(
                state_module_member
            ))
        else:
            pass

    return StateMetadata(get_state_dir(state),
                         state,
                         columns,
                         vote_columns,
                         df_transformers,
                         row_cleaners)


def get_state_dir(state: str) -> str:
    return os.path.join(BASE_DIR, 'openelections-data-{}'.format(state))


def get_state_module_path(state: str) -> str:
    return 'airflow_dags.open_elections.states.{}'.format(state)


def _combine_helper(left: Optional[list], right: Optional[list]):
    if left and not right:
        return left
    elif not left and right:
        return right
    else:
        return left + right


STATES = [
    'al',
    'ak',
    'az',
    'ar',
    'ca',
    'co',
    'ct',
    'de',
    'fl',
    'ga',
    'hi',
    'id',
    'il',
    'in',
    'ia',
    'ks',
    'ky',
    'la',
    'me',
    'md',
    'ma',
    'mi',
    'mn',
    'ms',
    'mo',
    'mt',
    'ne',
    'nv',
    'nh',
    'nj',
    'nm',
    'ny',
    'nc',
    'nd',
    'oh',
    'ok',
    'or',
    'pa',
    'ri',
    'sc',
    'sd',
    'tn',
    'tx',
    'ut',
    'vt',
    'va',
    'wa',
    'wv',
    'wi',
    'wy',
]