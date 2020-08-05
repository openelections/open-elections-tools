from open_elections.tools import StateDataFormat


INVALID_FILES = [
    '20140826__az__primary__greenlee__precinct.csv',
    '20140826__az__primary__coconino__precinct.csv',
    '20140826__az__primary__gila__precinct.csv',
    '20140826__az__primary__yuma__precinct.csv',
    '20140826__az__primary__pinal__precinct.csv',
    '20140826__az__primary__maricopa__precinct.csv',
    '20141104__az__general__yuma__precinct.csv',
    '20140826__az__primary__mohave__precinct.csv',
    '20140826__az__primary__apache__precinct.csv',
    '20141104__az__general__gila__precinct.csv',
    '20141104__az__general__pinal__precinct.csv',
    '20141104__az__general__santa_cruz__precinct.csv',
    '20141104__az__general__coconino__precinct.csv',
    '20141104__az__general__greenlee__precinct.csv',
    '20141104__az__general__pima__precinct.csv',
    '20141104__az__general__maricopa__precinct.csv',
    '20140826__az__primary__pima__precinct.csv',
    '20141104__az__general__la_paz__precinct.csv',
    '20141104__az__general__mohave__precinct.csv',
    '20141104__az__general__apache__precinct.csv',
    '20160830__az__primary__graham__precinct.csv',
    '20160830__az__primary__santa_cruz__precinct.csv',
    '20160830__az__primary__la_paz__precinct.csv',
    '20160322__az__primary__president__la_paz__precinct.csv',
    '20160322__az__primary__president__apache__precinct.csv',
    '20160830__az__primary__apache__precinct.csv',
    '20160322__az__primary__president__cochise__precinct.csv',
    '20160830__az__primary__mohave__precinct.csv',
    '20160322__az__primary__president__mohave__precinct.csv',
    '20160830__az__primary__yuma__precinct.csv',
    '20160322__az__primary__president__pinal__precinct.csv',
    '20160322__az__primary__president__gila__precinct.csv',
    '20160830__az__primary__gila__precinct.csv',
    '20160322__az__primary__president__yuma__precinct.csv',
    '20160830__az__primary__navajo__precinct.csv',
    '20160830__az__primary__yavapai__precinct.csv',
    '20160322__az__primary__president__coconino__precinct.csv',
    '20160830__az__primary__pima__precinct.csv',
    '20160322__az__primary__president__greenlee__precinct.csv',
    '20160830__az__primary__pinal__precinct.csv',
    '20160322__az__primary__president__maricopa__precinct.csv',
    '20160322__az__primary__president__pima__precinct.csv',
    '20160322__az__primary__president__santa_cruz__precinct.csv',
    '20160830__az__primary__maricopa__precinct.csv',
    '20160830__az__primary__cochise__precinct.csv',
    '20160830__az__primary__coconino__precinct.csv',
    '20160830__az__primary__greenlee__precinct.csv'
]


national_precinct_dataformat = StateDataFormat(
    excluded_files=INVALID_FILES
)