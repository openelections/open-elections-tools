from open_elections.tools import StateDataFormat

INVALID_VOTES_VALUES = [
'Moody', 'Hanson', 'McPherson', 'Hamlin', 'McCook', 'Day',
       'Harding', 'Brule', 'Grant', 'Hutchinson', 'Jones', 'Gregory',
       'Jerauld', 'Jackson', 'Edmunds', 'Campbell', 'Buffalo', 'Aurora',
       'Mellette', 'Haakon'
]


def remove_invalid_vote_counts(dic: dict):
    value = dic['votes']
    if type(value) == str and value in INVALID_VOTES_VALUES:
        dic['votes'] = None


national_precinct_dataformat = StateDataFormat(
    row_cleaners=[remove_invalid_vote_counts]
)
