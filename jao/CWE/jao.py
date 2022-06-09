import requests
import pandas as pd
from .exceptions import ServerReturnedEmptyData, InvalidCaptcha
from datetime import date
from suds.client import Client as suds_Client
from functools import wraps
from PIL import Image
from io import BytesIO, StringIO
from .parsers import _parse_utility_tool_xml, _parse_maczt_final_flowbased_domain, \
    _parse_utilitytool_xml, _parse_suds_tradingdata
import numpy as np
from .definitions import ParseDataSubject


class JaoUtilityToolASMXClient:
    # from the ASMX Web Service API, this is a very good defined system
    #   which supplies the endpoint and formats in xml upfront. this is delegate to the suds package
    #   for convenience some methods are wrapped into a dataframe

    def __init__(self):
        # all communication goes through the sud package, since the schemas are defined in the WSDL
        self.client = suds_Client("http://utilitytool.jao.eu/CascUtilityWebService.asmx?WSDL")

    def help(self):
        """
        this prints the scheme as defined by the WSDL format and parsed by the suds package

        :return:
        """
        print(str(self.client))

    def query_minmax_NP(self, d_from: str, d_to: str) -> pd.DataFrame:
        """

        :param d_from: start date string that is accepted by pandas timestamp
        :param d_to: end date string that is accepted by pandas timestamp
        """
        d_from = pd.Timestamp(d_from).strftime("%Y-%m-%d")
        d_to = pd.Timestamp(d_to).strftime("%Y-%m-%d")
        return _parse_suds_tradingdata(
            self.client.service.GetTradingDataForAPeriod(d_from, d_to, False, True, False),
            'MaxNetPositions',
            True
        )

    def query_max_bex(self, d_from: str, d_to: str) -> pd.DataFrame:
        """

        :param d_from: start date string that is accepted by pandas timestamp
        :param d_to: end date string that is accepted by pandas timestamp
        """
        d_from = pd.Timestamp(d_from).strftime("%Y-%m-%d")
        d_to = pd.Timestamp(d_to).strftime("%Y-%m-%d")
        return _parse_suds_tradingdata(
            self.client.service.GetTradingDataForAPeriod(d_from, d_to, True, False, False),
            'MaxExchanges',
            True
        )

    def query_CWE_NP(self, d_from: str, d_to: str) -> pd.DataFrame:
        """

        :param d_from: start date string that is accepted by pandas timestamp
        :param d_to: end date string that is accepted by pandas timestamp
        """
        d_from = pd.Timestamp(d_from).strftime("%Y-%m-%d")
        d_to = pd.Timestamp(d_to).strftime("%Y-%m-%d")
        return _parse_suds_tradingdata(
            self.client.service.GetNetPositionDataForAPeriod(d_from, d_to),
            'NetPositionData'
        )


class JaoUtilityToolCSVClient:
    # this ingests from the same client the excel macros in the utility tool is talking too
    # because of this the endpoints are open and thus no key or captcha is required
    def __init__(self):
        self.s = requests.Session()
        self.s.headers.update({
            'user-agent': 'jao-py (github.com/fboerman/jao-py)'
        })

    def query_cwe_net_position(self, d_from: str, d_to: str) -> pd.DataFrame:
        """
        Downloads the internal cwe net positions between the given date range

        :param d_from: start date string that is accepted by pandas timestamp
        :param d_to: end date string that is accepted by pandas timestamp
        :return:
        """
        d_from = pd.Timestamp(d_from)
        d_to = pd.Timestamp(d_to)
        url = f"https://utilitytool.jao.eu/WebServiceV2.asmx/GetNetPositionDataForAPeriod?" \
              f"dateFrom={d_from.strftime('%m-%d-%Y')}&dateTo={d_to.strftime('%m-%d-%Y')}"
        r = self.s.get(url)
        r.raise_for_status()

        return _parse_utilitytool_xml(r.text, 'NetPositionData', ['AT', 'NL', 'BE', 'DE', 'FR', 'ALBE', 'ALDE'], 'CalendarDate')

    def query_cwe_minmax_NP(self, d_from: str, d_to: str) -> pd.DataFrame:
        d_from = pd.Timestamp(d_from)
        d_to = pd.Timestamp(d_to)
        url = f"https://utilitytool.jao.eu/WebServiceV2.asmx/GetTradingDataForAPeriod?" \
              f"dateFrom={d_from.strftime('%m-%d-%Y')}&dateTo={d_to.strftime('%m-%d-%Y')}" \
              f"&maxExchange=false&netPosition=true&ptdf=false"
        r = self.s.get(url)
        r.raise_for_status()

        return _parse_utilitytool_xml(r.text, 'MaxNetPosition',
                                      ['MinAT', 'MaxAT', 'MinNL', 'MaxNL', 'MinBE', 'MaxBE', 'MinDE', 'MaxDE', 'MinFR',
                                       'MaxFR', 'MinALBE', 'MaxALBE', 'MinALDE', 'MaxALDE'], 'Date', xpath='ns:MaxNetPositions/')

    def _parse_domain(self, r: requests.Response) -> pd.DataFrame:
        # do some character formatting so pandas understands the text
        lines = r.text.replace(';|', "|").split('\r\n')
        lines[0] = lines[0].replace(';', '|')
        # load it in a virtual file and give it to pandas
        stream = StringIO()
        stream.write("\n".join(lines))
        stream.seek(0)

        df = pd.read_csv(stream, sep="|")

        # check if the dataframe is empty, this should not happen. default flow parameters always return something.
        # throw an error and let the user deal with it
        if len(df) == 0:
            raise ServerReturnedEmptyData

        # parse the date string which is only the day
        df['DeliveryDate'] = pd.to_datetime(df['DeliveryDate'], format='%d/%m/%Y %H:%M:%S')
        # insert the hour into the date by combining with the Period column

        # for DST: difficult problem. JAO gives everything in localtime, so we cant use the easy trick of everything in UTC and then later convert
        # it also gives the hour only as a period number for that day
        # for now this package solves it the following way: throwing away the double hour in case of clock going backwards
        # and leaving the hour empty when clock is going forward.
        # If a reader knows a better way please open an issue or pullrequest on github
        if df['Period'].max() > 24:
            # clock going backwards so one extra hour at hour
            def _shift_hour(row):
                if row['Period'] < 4: # before clock change
                    return row['DeliveryDate'].replace(hour=row['Period'] - 1)
                elif row['Period'] == 4: # during clock change
                    return np.nan
                elif row['Period'] > 4: # after clock change
                    return row['DeliveryDate'].replace(hour=row['Period'] - 2)
            df['DeliveryDate'] = df.apply(_shift_hour, axis=1)
        elif df['Period'].max() < 24:
            # clock going forward so one hour less
            def _shift_hour(row):
                if row['Period'] < 3: # before clock change
                    return row['DeliveryDate'].replace(hour=row['Period'] - 1)
                elif row['Period'] >= 3: # after clock change
                    return row['DeliveryDate'].replace(hour=row['Period'])
            df['DeliveryDate'] = df.apply(_shift_hour, axis=1)
        else:
            # normal time
            df['DeliveryDate'] = df.apply(lambda row: row['DeliveryDate'].replace(hour=row['Period'] - 1), axis=1)
        df = df.dropna(subset=['DeliveryDate'])
        df = df.rename(columns={'DeliveryDate': 'timestamp'}).drop(columns=['Period']).set_index('timestamp')
        df = df.tz_localize('Europe/Amsterdam', ambiguous=True)

        # now do some cleanup, remove useless columns and make the ptdf columns more efficient
        df.drop(columns=['FileId', 'Row'], inplace=True)
        # for ptdf columns we assume the jao system that the same bidding zone is mentioned in whole column
        # so only check first entry
        ptdf_translation = {}
        useless_columns = []
        i = 0
        while True:
            if i == 0:
                c1 = "Factor"
                c2 = "BiddingArea_Shortname"
            else:
                c1 = f"Factor.{i}"
                c2 = f"BiddingArea_Shortname.{i}"
            if c1 in df.columns and c2 in df.columns:
                ptdf_translation[c1] = f"PTDF_{df[c2].iloc[0]}"
                useless_columns.append(c2)
            else:
                break
            i += 1
        df = df.rename(columns=ptdf_translation)
        df = df.drop(columns=useless_columns)
        df = df.rename(columns={
                'OutageName': 'CO',
                'OutageEIC': 'CO_EIC',
                'CriticalBranchName': 'CNE',
                'CriticalBranchEIC': 'CNE_EIC',
                'RemainingAvailableMargin': 'RAM'
            })

        return df

    def query_final_flowbased_domain(self, d: str) -> pd.DataFrame:
        """
        Downloads the final flowbased of the business day of the given date object
        returns a dataframe with the data. This endpoint is relatively slow so we download one day per request
        for multiple days call this function repeatedly and pd.concat the dataframes

        :param d: date string that is accepted by pandas timestamp
        """
        d = pd.Timestamp(d)

        # retrieve the data from jao network call
        url = f"https://utilitytool.jao.eu/CSV/GetAllCBCOFixedLabelDataForAPeriod?dateFrom={d.strftime('%m-%d-%Y')}&" \
              f"dateTo={d.strftime('%m-%d-%Y')}&random=1"
        r = self.s.get(url)
        # check for http errors
        r.raise_for_status()

        return self._parse_domain(r)

    def query_initial_virgin_domain(self, d: str) -> pd.DataFrame:
        """
        Works the same as query_final_flowbased_domain but for the virgin domain initial computation

        :param d: date string that is accepted by pandas timestamp
        """
        d = pd.Timestamp(d)

        # retrieve the data from jao network call
        url = f"https://utilitytool.jao.eu/CSV/GetVirginDomainInitialComputationDataForAPeriod?"\
              f"dateFrom={d.strftime('%m-%d-%Y')}&" \
              f"dateTo={d.strftime('%m-%d-%Y')}&random=1"
        r = self.s.get(url)
        # check for http errors
        r.raise_for_status()

        return self._parse_domain(r)

    def query_final_virgin_domain(self, d: str) -> pd.DataFrame:
        """
        Works the same as query_final_flowbased_domain but for the virgin domain final computation

        :param d: date string that is accepted by pandas timestamp
        """
        d = pd.Timestamp(d)

        # retrieve the data from jao network call
        url = f"https://utilitytool.jao.eu/CSV/GetVirginDomainFinalComputationDataForAPeriod?"\
              f"dateFrom={d.strftime('%m-%d-%Y')}&" \
              f"dateTo={d.strftime('%m-%d-%Y')}&random=1"
        r = self.s.get(url)
        # check for http errors
        r.raise_for_status()

        return self._parse_domain(r)

    def query_maczt(self, d: str, zone: str = 'NL') -> pd.DataFrame:
        """
        Extract the MACZT numbers from the final flowbased domain.
        Calls the query_final_flowbased_domain function and has thus the same limitation about
                being relatively slow and only one day per call
        For now only the NL zone is supported

        :param d: date string that is accepted by pandas timestamp
        :param zone: str of the selected zone
        :return:
        """

        if zone != 'NL':
            raise NotImplementedError

        return _parse_maczt_final_flowbased_domain(
            self.query_final_flowbased_domain(d),
            zone=zone
        )


def captcha(func):
    """
    check if captcha is already solved and otherwise request one and solve it

    :param func:
    :return:
    """
    @wraps(func)
    def captcha_wrapper(*args, **kwargs):
        self = args[0]

        # atm the only check is if we have ever done the captcha
        #  later add checks if it has expired (does it even expire?)
        if self.captcha is None:
            r = self.s.get(self.BASEURL + "/Captcha/Show")
            r.raise_for_status()
            png_stream = BytesIO(r.content)
            png_stream.seek(0)
            captcha_png = Image.open(png_stream)
            captcha_png.show()
            captcha_code = input("Captcha: ")
            r = self.s.get(self.BASEURL + "/Util/Validate", params={
                'captchaValue': captcha_code
            })
            if r.status_code != 200:
                raise InvalidCaptcha
            if r.text != 'True':
                raise InvalidCaptcha
            self.captcha = captcha_code

        return func(*args, **kwargs)

    return captcha_wrapper


class JaoUtilityToolXmlClient:
    # uses the xml download from the utility tool website at https://www.jao.eu/implict-allocation
    # this requires solving a recaptcha by the user

    BASEURL = "https://utilitytool.jao.eu"

    def __init__(self):
        self.s = requests.Session()
        self.s.headers.update({
            'user-agent': 'jao-py (github.com/fboerman/jao-py)'
        })
        self.captcha = None

    @captcha
    def query_xml(self, date_from: str, date_to: str) -> bytes:
        """
        download the utility tool xml file with given dates, returns the raw xml in bytes

        :param date_from: start date string that is accepted by pandas timestamp
        :param date_to: end date string that is accepted by pandas timestamp
        :return:
        """

        date_from = pd.Timestamp(date_from)
        date_to = pd.Timestamp(date_to)

        r = self.s.post(self.BASEURL + '/Util/Download', data={
            'Date': date.today().strftime("%Y/%m/%d"),
            'fileType': 'Xml',
            'FromDate': date_from.strftime("%Y/%m/%d"),
            'ToDate': date_to.strftime("%Y/%m/%d"),
            'CaptchaValue': self.captcha,
            'InvisibleCaptchaValue': '',
            'force': 'false',
        })

        r.raise_for_status()

        return r.content

    def query_df(self, date_from: str, date_to: str, t: ParseDataSubject) -> pd.DataFrame:
        """
        downloads the utility tool xml file and parses the selected data type from the response into a dataframe

        :param date_from: start date string that is accepted by pandas timestamp
        :param date_to: end date string that is accepted by pandas timestamp
        :param t: which type to parse, choose from ParseDataSubject Enum
        :return:
        """

        return _parse_utility_tool_xml(self.query_xml(date_from, date_to), t)
