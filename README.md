# jao-py
![tests](https://github.com/fboerman/jao-py/actions/workflows/run-tests.yml/badge.svg)

Python client for the various endpoints offered by jao.eu, the Joint Allocation Office.
"Europe's single leading trading platform( e-CAT) for cross-border transmission capacity"

More information about JAO can be found on their website https://jao.eu/

jao.eu has various ways of retrieving data. This package tries to offer useful functions to handle them.
This package is not exhaustive, more methods are added when the authors needs them.
If you want to see other methods added please either open a feature request issue to give others ideas or 
supply a pull request yourself.


## Installation
`python3 -m pip install jao-py`

## Usage
### Current clients
The package comes with the current pandas clients:
- [`JaoAPIClient`](#JaoAPIClient): api client for the webservice API defined [here](https://www.jao.eu/page-api/market-data)
- [`JaoPublicationToolClient`](#JaoPublicationToolClient): client for the Core Day-Ahead publication tool defined [here](https://publicationtool.jao.eu/core/)
- [`JaoPublicationToolPandasIntraDay`](#JaoPublicationToolPandasIntraDay): client for Core Intradaypublication tool for Intraday defined [here](https://publicationtool.jao.eu/coreID/)
- [`JaoPublicationToolPandasNordics`](#JaoPublicationToolPandasNordics): client for Nordic Day-Ahead publication tool defined [here](https://publicationtool.jao.eu/nordic/)
The publication tool clients have valid data from their respective go lives:
- Core Day-Ahead: business day 2022-06-09 onwards
- Core Intraday(b): business day 2024-05-29 onwards
- Core Intraday(a): business day 2024-06-14 onwards
- Nordic Day-Ahead: business day 2024-10-30 onwards

Core Intraday a and b are combined in the same intraday client. In the initialization of the client you can choose which one you want like so:
```python
from jao import JaoPublicationToolPandasIntraDay

client = JaoPublicationToolPandasIntraDay(version='a') # IDCC(a)
client = JaoPublicationToolPandasIntraDay(version='b') # IDCC(b)
client = JaoPublicationToolPandasIntraDay(version='c') # IDCC(c)
```

There is also a client available for parallel run, which works in the exact same way as the intraday client above:
```python
from jao import JaoPublicationToolPandasIntraDayParRun

client = JaoPublicationToolPandasIntraDayParRun('c') #IDCC(c)
```

### Deprecated clients
The package also includes legacy clients for flowbased CWE data in the CWE subpackage. These return data up until business day 2022-06-08
- [`JaoUtilityToolASMXClient`](#JaoUtilityToolASMXClient): a very light wrapper around the ASMX Web Service API implemented as a passthrough to the suds-community pakcage
- [`JaoUtilityToolCSVClient`](#JaoUtilityToolCSVClient): client to download csv data inm the same way as the utility tool excel file, returns pandas dataframes
- [`JaoUtilityToolXmlClient`](#JaoUtilityToolXmlClient): downloads xml data of the utilitytool, this requires solving a captcha by the user, returns pandas dataframes

To use these deprecated clients be sure to install the following additional dependencies:
```
suds-community
lxml
pillow
```
this is only required for the CWE deprecated subpackage