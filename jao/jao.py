import requests
import pandas as pd
from .exceptions import *
from datetime import timedelta, date
from calendar import monthrange
from dateutil.relativedelta import relativedelta
from suds.client import Client as suds_Client
from functools import wraps
from PIL import Image
from io import BytesIO, StringIO
from .parsers import _parse_utility_tool_xml, _parse_maczt_final_flowbased_domain


__title__ = "jao-py"
__version__ = "0.1.3"
__author__ = "Frank Boerman"
__license__ = "MIT"


class JaoAPIClient:
    # this pulls from the same api as the frontend pulls from
    # requires no further api key

    BASEURL = "https://www.jao.eu/api/v1"

    def __init__(self):
        self.s = requests.Session()
        self.s.headers.update({
            'user-agent': 'jao-py (github.com/fboerman/jao-py)'
        })

    def query_auction_corridors(self):
        r = self.s.post(self.BASEURL + '/auction/calls/getcorridors', json={})
        if r.status_code != 200:
            raise ServerReturnsInvalidStatusCode
        return [x['value'] for x in r.json()]

    def query_auction_horizons(self):
        r = self.s.post(self.BASEURL + '/auction/calls/gethorizons', json={})
        if r.status_code != 200:
            raise ServerReturnsInvalidStatusCode
        return [x['value'] for x in r.json()]

    def query_auction_details_by_month(self, corridor, month):
        """
        get the auction data for a specified month. gives basically everything but the bids themselves

        :param corridor: string of a valid jao corridor from query_corridors
        :param month: datetime.date object for the month you want the auction data of
        :return:
        """
        # prepare the specific input arguments needed, start day the day before the months begin
        # end date the last day of the month

        month_begin = month.replace(day=1)
        month_end = month.replace(day=monthrange(month.year, month.month)[1])
        r = self.s.post(self.BASEURL + '/auction/calls/getauctions', json={
            'corridor': corridor,
            'fromdate': (month_begin - timedelta(days=1)).strftime("%Y-%m-%d"),
            'horizon': 'Monthly',
            "todate": month_end.strftime("%Y-%m-%d")})

        if r.status_code != 200:
            raise ServerReturnsInvalidStatusCode

        data = r.json()
        # pretify the results since we know it is for monthly auction
        data = data[0]
        data = {**data, **data['results'][0], **data['products'][0]}
        del data['results']
        del data['products']

        return data

    def query_auction_bids_by_month(self, corridor, month, as_dict=False):
        """
        wrapper function to construct the auction id since its predictable and
          pass it on to the query function for the bids

        :param corridor: string of a valid jao corridor from query_corridors
        :param month: datetime.date object for the month you want the auction bids of
        :param as_dict: boolean wether the result needs to be returned as a dictionary instead of a dataframe
        :return:
        """

        return self.query_auction_bids_by_id(month.strftime(f"{corridor}-M-BASE-------%y%m01-01"), as_dict=as_dict)

    def query_auction_bids_by_id(self, auction_id, as_dict=False):
        r = self.s.post(self.BASEURL + "/auction/calls/getbids", json={
            "auctionid": auction_id
        })

        if r.status_code != 200:
            raise ServerReturnsInvalidStatusCode

        if as_dict:
            return r.json()
        else:
            return pd.DataFrame(r.json())


    def query_auction_stats_months(self, month_from, month_to, corridor):
        """
        gets the following statistics for the give range of months (included both ends) in a dataframe:
        id
        corridor
        month
        auction start
        auction ended
        offered capacity (MW)
        ATC (MW)
        allocated capacity (MW)
        resold capacity (MW)
        requested capacity (MW)
        price (EUR/MW)
        non allocated capacity (MW)
        in this order

        :param month_from: datetime.date object of start month
        :param month_to: datetime.date object of end month
        :param corridor: string of a valid jao corridor from query_corridors
        :return:
        """
        detail_keys = ['bidGateOpening', 'bidGateClosure', 'offeredCapacity', 'atc',
                        'allocatedCapacity', 'resoldCapacity', 'requestedCapacity', 'auctionPrice']

        data = []
        m = month_from
        while m <= month_to:
            m_details = self.query_auction_details_by_month(corridor, m)
            m_data = {
                'id': m_details['identification'],
                'corridor': corridor,
                'month': m.replace(day=1)
            }
            m_data = {**m_data, **{k: v for k, v in m_details.items() if k in detail_keys}}
            data.append(m_data)
            m += relativedelta(months=1)
        df = pd.DataFrame(data)
        df['resoldCapacity'].fillna(0, inplace=True)
        df['nonAllocatedCapacity'] = df['offeredCapacity'] - df['allocatedCapacity']

        return df


class JaoUtilityToolASMXClient:
    # from the ASMX Web Service API, this is a very good defined system
    #   which supplies the endpoint and formats in xml upfront. this is delegate to the suds package
    #   so this is only a very simple wrapper
    # TODO: some known methods are wrapped so that they return a proper dataframe

    def __init__(self):
        # all communication goes through the sud package, since the schemas are defined in the WSDL
        self.client = suds_Client("http://utilitytool.jao.eu/CascUtilityWebService.asmx?WSDL")

    def help(self):
        """
        this prints the scheme as defined by the WSDL format and parsed by the suds package

        :return:
        """
        print(str(self.client))


class JaoUtilityToolCSVClient:
    # this ingests from the same client the excel macros in the utility tool is talking too
    # because of this the endpoints are open and thus no key or captcha is required
    def __init__(self):
        self.s = requests.Session()
        self.s.headers.update({
            'user-agent': 'jao-py (github.com/fboerman/jao-py)'
        })

    def query_final_flowbased_domain(self, d):
        """
        Downloads the final flowbased of the business day of the given date object
        returns a dataframe with the data. This endpoint is relatively slow so we download one day per request
        for multiple days call this function repeatedly and pd.concat the dataframes

        :param d: datetime.date object
        """

        url = f"https://utilitytool.jao.eu/CSV/GetAllCBCOFixedLabelDataForAPeriod?dateFrom={d.strftime('%m-%d-%Y')}&" \
              f"dateTo={d.strftime('%m-%d-%Y')}&random=1"
        r = self.s.get(url)

        lines = r.text.replace(';|', "|").split('\r\n')
        lines[0] = lines[0].replace(';', '|')

        lines = r.text.replace(';|', "|").split('\r\n')
        lines[0] = lines[0].replace(';', '|')

        stream = StringIO()
        stream.write("\n".join(lines))
        stream.seek(0)

        df = pd.read_csv(stream, sep="|")

        df['DeliveryDate'] = pd.to_datetime(df['DeliveryDate'], format='%d/%m/%Y %H:%M:%S')
        df['DeliveryDate'] = df.apply(lambda row: row['DeliveryDate'].replace(hour=row['Period'] - 1), axis=1)
        df = df.rename(columns={'DeliveryDate': 'timestamp'}).drop(columns=['Period']).set_index('timestamp')
        df = df.tz_localize('Europe/Amsterdam')

        return df

    def query_maczt(self, d, zone='NL'):
        """
        Extract the MACZT numbers from the final flowbased domain.
        Calls the query_final_flowbased_domain function and has thus the same limitation about
                being relatively slow and only one day per call
        For now only the NL zone is supported

        :param d: datetime.date object
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
            if r.status_code != 200:
                raise ServerReturnsInvalidStatusCode
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
    def query_xml(self, date_from, date_to):
        """
        download the utility tool xml file with given dates, returns the raw xml

        :param date_from:
        :param date_to:
        :return:
        """
        r = self.s.post(self.BASEURL + '/Util/Download', data={
            'Date': date.today().strftime("%Y/%m/%d"),
            'fileType': 'Xml',
            'FromDate': date_from.strftime("%Y/%m/%d"),
            'ToDate': date_to.strftime("%Y/%m/%d"),
            'CaptchaValue': self.captcha,
            'InvisibleCaptchaValue': '',
            'force': 'false',
        })

        if r.status_code != 200:
            raise ServerReturnsInvalidStatusCode

        return r.content

    def query_df(self, date_from, date_to, t):
        """
        downloads the utility tool xml file and parses the selected data type from the response into a dataframe

        :param date_from:
        :param date_to:
        :param t: which type to parse, choose from "MaxExchanges", "MaxNetPositions", "Ptdfs"
        :return:
        """

        return _parse_utility_tool_xml(self.query_xml(date_from, date_to), t)
