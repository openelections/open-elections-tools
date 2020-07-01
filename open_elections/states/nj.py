from open_elections.tools import StateDataFormat


def remove_invalid_vote_counts(dic: dict):
    value = dic['votes']
    if value in ['70S', '96S', '60S', '54S', '53S', '1 1', 'Ballots Cast', 'Votes']:
        dic['votes'] = None


national_precinct_dataformat = StateDataFormat(
    row_cleaners=[remove_invalid_vote_counts]
)
