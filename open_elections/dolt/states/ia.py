from open_elections.tools import StateDataFormat


def remove_invalid_vote_counts(dic: dict):
    value = dic['votes']
    if value in [' ', 'None', '?', 'O', '[1']:
        dic['votes'] = None


national_precinct_dataformat = StateDataFormat(
    row_cleaners=[remove_invalid_vote_counts]
)
