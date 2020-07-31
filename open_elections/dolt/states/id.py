from open_elections.tools import StateDataFormat


def remove_emtpy_string_vote_counts(dic: dict):
    if dic['votes'] == ' ':
        dic['votes'] = None


national_precinct_dataformat = StateDataFormat(
    row_cleaners=[remove_emtpy_string_vote_counts]
)