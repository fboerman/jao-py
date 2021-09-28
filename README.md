# jao-py
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
The package comes with 4 clients:
- [`JaoAPIClient`](#JaoAPIClient): api client for the normal http api
- [`JaoUtilityToolASMXClient`](#JaoUtilityToolASMXClient): a very light wrapper around the ASMX Web Service API implemented as a passthrough to the suds-community pakcage
- [`JaoUtilityToolCSVClient`](#JaoUtilityToolCSVClient): client to download csv data inm the same way as the utility tool excel file, returns pandas dataframes
- [`JaoUtilityToolXmlClient`](#JaoUtilityToolXmlClient): downloads xml data of the utilitytool, this requires solving a captcha by the user, returns pandas dataframes
