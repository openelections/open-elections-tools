## Background
There are, broadly speaking, two kinds of "correctness" to consider in "data" of this kind, that is "collections of facts" structured for consumption. The first is technical, and corresponds to questions such as 
- does each line have the same number of values?
- are all the voting columns integer values?

The second type of correctness concerns the actual values in the data. For example, the value `'xdtoav'` given for the column `county` is a valid value for type `str`, but does not correspond to a county in the United States. This latter type of correctness is much harder to establish, and often impossible to test for. 

This document details the tooling we have built around improving the chances of Open Elections CSVs containing valid and correct data.

### Format and Technical Correctness
Open Elections data is posted to state level repositories in the form of CSV files. CSV is a "loose" format, in the sense that it's up to the user to format the file according to the "standard", but there is no tooling around the format to prevent files from being formatted incorrectly. This is an inherent shortcoming with formats, they don't come with "forcing functions" for driving technical correctness.

### Data Validation
A more difficult kind of correctness to establish is the actual validity of our data. Returning to the example of a nonsensical value for a county column, we might write a test that queries some "Oracle" that is trusted to establish the correctness of the data, or other cannonical data source. 

## Instrumentation
We currently expose a simple set of tests defined in `open_elections/validation/data_issues_by_state.py` as both a command line tool and a via a `pytest` shell command. The former is for contributors to validate their work, the latter is for CI tooling to validate individual repositories. The shell command can be run as follows, after first installing the package:
```
$ pip install open-elections
.
.
.
$ validate-state --years 2018 --base-dir path/to/openelections-data-pa --state PA
```

`validate-state` is a generated shim that resolves to a script which parses the arguments and executes the checks.