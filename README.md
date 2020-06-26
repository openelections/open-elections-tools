## Background
Open Elections is a project that seeks to gather all data on all legislative and executive branch elections, at the state and federal level, into a single place. That place is a set of GitHub repositories. The repostiories are structured such that there is a mapping between data publishing locale, and file, varying based on the reporting mechanisms and granularity of each state.

We think it would be great if instead of having to download the appropriate CSVs via API calls and the like, we could actually turn this into a coherent SQL database. It would be a unique and differentiated dataset, and allow users to make powerful queries against the data.

## Configuration
There is really only one necessary global configuration, the location of the state level data repositories mentioned above. An example might be:
```
/Users/you/Documents/open-elections/
```
Then acquire, for example, California data like so:
```
$ cd /Users/you/Documents/open-elections/ && git clone https://github.com/openelections/openelections-data-ca.git
``` 

Make the appropriate assignment in `open_elections.config`:
```
BASE_DIR = /Users/you/Documents/open-elections/
```

The rest of the tools take a state and then just build a path from this value, assuming the subdirectories are the same as the GitHub repos they correspond to.


## Code Organization
As mentioned in the background we aim to collect two kinds of data: data that is uniformily reported at the national level, such as a precinct level vote totals, and then state sepcific breakdowns that break out tabulation buckets (mail in voting, etc.), at the precinct level. Each state provides a different set of tabulation buckets, so we have to have state specific tables for the breakdowns.

### `open_elections/tools.py`
This contains tools for traversing a state data repository and extracting a data table. Three important objects are defined:
```
StateMetadata
    - This contains all the necessary information for how to parse a vote file for a given state. It has members that
      allow you to specify how to correct certain corruption in the raw data, and how to define state specific logic 
      for parsing. For example, we can tell Python to convert "1,000" to "1000" which then parses as an integer.
StateDataFormat
    - This is a configuration class that is used to specify a state's custom rules. These are defined in the state 
      specific modules and loaded when building StateMetadata instances. The idea is that each state can be parsed 
      according to the idiosyncratic set of rules that went into it. 
VoteFile
    - The VoteFile wraps the atomic data unit in this project.

```

The most important function in this module brings these objects together to create table data:
```python
def files_to_table_data(state_metadata: StateMetadata,
                        vote_file_builder: VoteFileBuilder,
                        table_data_builder: Callable[[pd.DataFrame], List[dict]]) -> List[dict]
```

The function does the following:
    - a `StateMetadata` instance is used to traverse the data repository
    - `vote_file_builder` uses those file paths to create `VoteFile` instances
    - `VoteFile.to_enriched_df()` returns a `pandas.DataFrame` for each `VoteFile`, these are combined to a single table for the whole state
    - `table_data_builder` maps those `pandfas.DataFrame` objects to a `List[dict]` where each row is a `dict`
    
Thus to import OpenElection data you need to do the following:
1. define which files you want to import by poroviding a function that returns a `VoteFile` or one of its subclasses.
2. optionally define a member of the state specific module that deals with special cases for state, for example Wisconsin refers to "precincts" as a "wards" so we define a rule that renames the column:
    ```
    national_precinct_dataformat = StateDataFormat(
        df_transformers=[lambda df: df.rename(columns={'ward': 'precinct'})]
    
    ```
   `open_elections.config.build_state_metadata` will ensure this is included in the returned `StateMetadata` instances `df_transformers` member. This transformer will then be applied when parsing the `VoteFile` instances that have a reference to that `StateMetadata`.
3. define a function that takes the state level `pandas.DataFrame` and maps it to a `List[dict]`

## National Precinct Level Data
Our first phase of this project is to gather precinct level voting tallies for all states in a single national table. Let's use parsing that dataset as an example. The following command will try and load the data for California:
```
$ python airflow_dags/open_elections/load_shared_voting_data.py --state ca --load-data --dolt-dir ~/Documents/open-elections
```

Before doing that however we should run our integrity checks. These parse the files using the exact settings the loader will, and report any issues:
```
>>> from open_elections.load_shared_voting_data import run_integrity_report
>>> file_issues, data_issues = run_integrity_report('ca')
>>> data_issues
    state                                           filename  ...  type_check value
0     ca  /Users/oscarbatori/Documents/open-elections/op...  ...       False     X
1     ca  /Users/oscarbatori/Documents/open-elections/op...  ...       False     X
2     ca  /Users/oscarbatori/Documents/open-elections/op...  ...       False     X
3     ca  /Users/oscarbatori/Documents/open-elections/op...  ...       False     X
4     ca  /Users/oscarbatori/Documents/open-elections/op...  ...       False     X
5     ca  /Users/oscarbatori/Documents/open-elections/op...  ...       False     X
6     ca  /Users/oscarbatori/Documents/open-elections/op...  ...       False     X
7     ca  /Users/oscarbatori/Documents/open-elections/op...  ...       False     X
0     ca  /Users/oscarbatori/Documents/open-elections/op...  ...       False     -
1     ca  /Users/oscarbatori/Documents/open-elections/op...  ...       False     -
2     ca  /Users/oscarbatori/Documents/open-elections/op...  ...       False     -
3     ca  /Users/oscarbatori/Documents/open-elections/op...  ...       False     -
4     ca  /Users/oscarbatori/Documents/open-elections/op...  ...       False     -
5     ca  /Users/oscarbatori/Documents/open-elections/op...  ...       False     -
6     ca  /Users/oscarbatori/Documents/open-elections/op...  ...       False     -
7     ca  /Users/oscarbatori/Documents/open-elections/op...  ...       False     -
8     ca  /Users/oscarbatori/Documents/open-elections/op...  ...       False     -
9     ca  /Users/oscarbatori/Documents/open-elections/op...  ...       False     -
10    ca  /Users/oscarbatori/Documents/open-elections/op...  ...       False     -
11    ca  /Users/oscarbatori/Documents/open-elections/op...  ...       False     -
12    ca  /Users/oscarbatori/Documents/open-elections/op...  ...       False     -
13    ca  /Users/oscarbatori/Documents/open-elections/op...  ...       False     -
14    ca  /Users/oscarbatori/Documents/open-elections/op...  ...       False     -
15    ca  /Users/oscarbatori/Documents/open-elections/op...  ...       False     -
[24 rows x 7 columns]
```

This tells us that the California data contains a handful of rows that have non-numeric placeholders for some form of "null", and in vote counting that's basically zero. In order to solve this we need to implement a state-specific row-cleaner in `open_elections.states.ca`:
```python
from open_elections.tools import StateDataFormat


def remove_invalid_vote_counts(dic: dict):
    value = dic['votes']
    if value in ('-', 'X'):
        dic['votes'] = None


national_precinct_dataformat = StateDataFormat(
    row_cleaners=[remove_invalid_vote_counts]
)
```

The `opne_elections.config.build_metadata` function takes an argument called `state_module_member` which will pick up that value. It's already set in `open_elections.load_shared_voting_data.build_metadata_helper` which inserts the necessary this name, as well as the column names, since they are shared across the state. Now, when we convert the data in the state-wide frame to rows, that is a list of dicts, this function will be applied. Note that we do the modification in-place for performance reasons.


## State Breakdowns
The next phase of this project will involve state level breakdowns.
