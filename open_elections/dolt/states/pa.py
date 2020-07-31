from open_elections.tools import StateDataFormat

INVALID_FORMAT_FILES = [
'20141104__pa__general__precinct.csv',
'20140520__pa__primary__precinct.csv',
'20121106__pa__general__precinct.csv',
'20120424__pa__primary__precinct.csv',
'20080422__pa__primary__precinct.csv',
'20081104__pa__general__precinct.csv',
'20060516__pa__primary__precinct.csv',
'20061107__pa__general__precinct.csv',
'20000404__pa__primary__precinct.csv',
'20001107__pa__general__precinct.csv',
'20100518__pa__primary__precinct.csv',
'20101102__pa__general__precinct.csv',
'20180517__pa__primary__precinct.csv',
'20160426__pa__primary__precinct.csv',
'20020521__pa__primary__precinct.csv',
'20021105__pa__general__precinct.csv',
'20040427__pa__primary__precinct.csv',
'20041102__pa__general__precinct.csv'
]

national_precinct_dataformat = StateDataFormat(
    excluded_files=INVALID_FORMAT_FILES
)