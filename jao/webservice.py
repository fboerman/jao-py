import requests
import pandas as pd
from datetime import timedelta, date
from calendar import monthrange
from dateutil.relativedelta import relativedelta
from typing import Union


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
        r.raise_for_status()
        return [x['value'] for x in r.json()]

    def query_auction_horizons(self):
        r = self.s.post(self.BASEURL + '/auction/calls/gethorizons', json={})
        r.raise_for_status()
        return [x['value'] for x in r.json()]

    def query_auction_details_by_month(self, corridor: str, month: date) -> dict:
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

        r.raise_for_status()

        data = r.json()
        # pretify the results since we know it is for monthly auction
        data = data[0]
        data = {**data, **data['results'][0], **data['products'][0]}
        del data['results']
        del data['products']

        return data

    def query_auction_bids_by_month(self, corridor: str, month: date, as_dict: bool = False) -> Union[pd.DataFrame, dict]:
        """
        wrapper function to construct the auction id since its predictable and
          pass it on to the query function for the bids

        :param corridor: string of a valid jao corridor from query_corridors
        :param month: datetime.date object for the month you want the auction bids of
        :param as_dict: boolean wether the result needs to be returned as a dictionary instead of a dataframe
        :return:
        """

        return self.query_auction_bids_by_id(month.strftime(f"{corridor}-M-BASE-------%y%m01-01"), as_dict=as_dict)

    def query_auction_bids_by_id(self, auction_id: str, as_dict: bool = False) -> Union[pd.DataFrame, dict]:
        r = self.s.post(self.BASEURL + "/auction/calls/getbids", json={
            "auctionid": auction_id
        })

        r.raise_for_status()

        if as_dict:
            return r.json()
        else:
            return pd.DataFrame(r.json())

    def query_auction_stats_months(self, month_from: date, month_to: date, corridor: str) -> pd.DataFrame:
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
