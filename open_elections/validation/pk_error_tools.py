from datetime import datetime
import logging
from typing import List

logger = logging.getLogger(__name__)

# TODO
#   temporary section to debug issues caused by seemingly bogus primary key error
test_data = {
    'state': "CA",
    'year': 2016,
    'date': datetime(2016, 11, 8),
    'election': "general",
    'special': False,
    'office': "State Assembly",
    'district': "10",
    'county': "Sonoma",
    'precinct': "7522",
    'party': "DEM",
    'candidate': "Marc Levine"
}


def parse_and_check_error(msg: str, data: List[dict]):
    """
    This is temporary function that takes a MySQL error messsage and
    :param msg:
    :param data:
    :return:
    """
    arr = msg.split('duplicate primary key given: ')[1].lstrip('((').rstrip('))').split(',')
    state_pos = 1
    year_pos = 3
    date_pos = 5
    election_pos = 7
    special_pos = 9
    office_pos = 11
    district_pos = 13
    county_pos = 15
    precinct_pos = 17
    party_pos = 19
    candidate_pos = 21
    count = 0
    t = tuple([arr[state_pos].strip('"'),
               int(arr[year_pos]),
               arr[date_pos],
               arr[election_pos].strip('"'),
               bool(int(arr[special_pos])),
               arr[office_pos].strip('"'),
               arr[district_pos].strip('"'),
               arr[county_pos].strip('"'),
               arr[precinct_pos].strip('"'),
               arr[party_pos].strip('"'),
               arr[candidate_pos].strip('"')])
    logger.warning('Checking for duplicate\n{}'.format(t))
    for dic in data:
        if (dic['state'] == arr[state_pos].strip('"') and
                dic['year'] == int(arr[year_pos]) and
                dic['election'] == arr[election_pos].strip('"') and
                dic['special'] == bool(int(arr[special_pos])) and
                dic['office'] == arr[office_pos].strip('"') and
                dic['district'] == arr[district_pos].strip('"') and
                dic['county'] == arr[county_pos].strip('"') and
                dic['precinct'] == arr[precinct_pos].strip('"') and
                dic['party'] == arr[party_pos].strip('"') and
                dic['candidate'] == arr[candidate_pos].strip('"')):
            count += 1

    logger.warning('Found records {}'.format(count))