# flowbased CWE
This subpackage includes the following clients:
- [`JaoUtilityToolASMXClient`](#JaoUtilityToolASMXClient): a very light wrapper around the ASMX Web Service API implemented as a passthrough to the suds-community pakcage
- [`JaoUtilityToolCSVClient`](#JaoUtilityToolCSVClient): client to download csv data inm the same way as the utility tool excel file, returns pandas dataframes
- [`JaoUtilityToolXmlClient`](#JaoUtilityToolXmlClient): downloads xml data of the utilitytool, this requires solving a captcha by the user, returns pandas dataframes

To use them please install the following extra dependencies:
```
suds-community
lxml
pillow
```

The use of these clients has been deprecated since the flowbased CORE day-ahead go live.
They return data up until business day 2022-06-08.