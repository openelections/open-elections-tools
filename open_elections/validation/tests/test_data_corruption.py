from open_elections.validation.data_issues_by_state import run_checks, display_exceptions


def test_state_data_corruption(base_dir, state, years):
    exceptions = run_checks(base_dir, state, years)
    display_exceptions(exceptions)
    assert not exceptions
