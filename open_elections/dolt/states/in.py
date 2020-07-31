from open_elections.tools import StateDataFormat


INVALID_VOTE_VALUES = [
    'Ballots',
    'Cast',
    'SEC',
    'Karl Tatgenhorst [L]',
    'AUD',
    'Suzanne Crocuh [R]',
    'AUD',
    'Michael Claytor [D]',
    'AUD',
    'John Schick [L]',
    'TREAS',
    'Kelly Mitchell [R]',
    'TREAS',
    'Mike Boland [D]',
    'TREAS',
    'Mike Jasper [L]',
    'SEN',
    'o',
    'o',
    '/46',
]


def remove_invalid_vote_counts(dic: dict):
    value = dic['votes']
    if value in INVALID_VOTE_VALUES:
        dic['votes'] = None


national_precinct_dataformat = StateDataFormat(
    row_cleaners=[remove_invalid_vote_counts]
)
