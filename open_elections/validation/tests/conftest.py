import pytest


def pytest_addoption(parser):
    parser.addoption('--years', type=str)
    parser.addoption('--state', type=str, required=True)
    parser.addoption('--base-dir', type=str, required=True)


@pytest.fixture
def base_dir(request):
    return request.config.getoption('base_dir')


@pytest.fixture
def state(request):
    return request.config.getoption('state')


@pytest.fixture
def years(request):
    return [int(year) for year in request.config.getoption('years').split(',')]


def _parameterize_helper(metafunc, param_name, param_value):
    if param_name in metafunc.fixturenames:
        metafunc.parametrize(param_name, param_value)
