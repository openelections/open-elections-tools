## Background
This repository backs a package on PyPi called [open-elections](https://pypi.org/project/open-elections/). It provides tools for working with Open Elections data. The repository is split into three packages.

`dolt` and `validation` submodules build on top of `tools` submodule to tools for validating data and writing it to Dolt. We hope to add more features over time.

### `open_elections.validation`
This provides tooling for validating Open Elections data submissions, and repos. There is a command line tool, and it is used in CI for validating new submissions.

### `open_elections.dolt`
This provides tools for loading Open Elections data to Dolt, and DoltHub. You can find the result repository [here](https://www.dolthub.com/repositories/open-elections/voting-data). It consists, currenlty, of a single table for nationwide precinct level voting totals.

### `open_elections.tools`
Tools for traversing Open Elections data repositories, extracting metadata from file names, and parsing the data.

## Issues
Please submit an issue if you find a bug or want to contribute a fix or feature.
