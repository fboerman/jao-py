import requests
import pandas as pd
from datetime import timedelta, date
from calendar import monthrange
from dateutil.relativedelta import relativedelta


class JaoAPIClient:
    # https://www.jao.eu/page-api/market-data

    BASEURL = "https://api.jao.eu/OWSMP/"

    def __init__(self, api_key):
        self.s = requests.Session()
        self.s.headers.update({
            'user-agent': 'jao-py (github.com/fboerman/jao-py)',
            'AUTH_API_KEY': api_key
        })

    def query_auction_corridors(self):
        r = self.s.get(self.BASEURL + 'getcorridors')
        r.raise_for_status()
        return [x['value'] for x in r.json()]

    def query_auction_horizons(self):
        r = self.s.get(self.BASEURL + 'gethorizons')
        r.raise_for_status()
        return [x['value'] for x in r.json()]

    def query_auction_details_by_month(self, corridor: str, month: date, horizon: str, shadow_auctions_only: bool = False) -> dict:
        """
        get the auction data for a specified month. gives basically everything but the bids themselves

        :param shadow_auctions_only: wether to only retrieve shadow auction results
        :param corridor: string of a valid jao corridor from query_corridors
        :param month: datetime.date object for the month you want the auction data of
        :param horizon: string object for the horizon you want the auction data of
        :return:
        """
        # prepare the specific input arguments needed, start day the day before the months begin
        # end date the last day of the month

        month_begin = month.replace(day=1)
        month_end = month.replace(day=monthrange(month.year, month.month)[1])
        if horizon == 'Yearly':
            r = self.s.get(self.BASEURL + 'getauctions', params={
                'corridor': corridor,
                'fromdate': month.strftime('%Y-%m-%d'),
                'horizon': horizon,
                'shadow': int(shadow_auctions_only)
            })
        else:
            r = self.s.get(self.BASEURL + 'getauctions', params={
                'corridor': corridor,
                'fromdate': (month_begin - timedelta(days=1)).strftime("%Y-%m-%d"),
                'horizon': horizon,
                'todate': month_end.strftime("%Y-%m-%d"),
                'shadow': int(shadow_auctions_only)
            })

        r.raise_for_status()

        data = r.json()
        # pretify the results since we know it is for monthly auction
        data = data[0]
        data = {**data, **data['results'][0], **data['products'][0]}
        del data['results']
        del data['products']

        return data

    def query_auction_bids_by_month(self, corridor: str, month: date, as_dict: bool = False) -> pd.DataFrame | dict:
        """
        wrapper function to construct the auction id since its predictable and
          pass it on to the query function for the bids

        :param corridor: string of a valid jao corridor from query_corridors
        :param month: datetime.date object for the month you want the auction bids of
        :param as_dict: boolean wether the result needs to be returned as a dictionary instead of a dataframe
        :return:
        """

        return self.query_auction_bids_by_id(month.strftime(f"{corridor}-M-BASE-------%y%m01-01"), as_dict=as_dict)

    def query_curtailments_by_month(self, corridor: str, month: date, as_dict: bool = False) -> pd.DataFrame | list:
        month_begin = month.replace(day=1)
        month_end = month.replace(day=monthrange(month.year, month.month)[1])
        r = self.s.get(self.BASEURL + 'getcurtailment', params={
            'corridor': corridor,
            'fromdate': (month_begin - timedelta(days=1)).strftime("%Y-%m-%d"),
            'todate': month_end.strftime("%Y-%m-%d"),
        })

        r.raise_for_status()
        if as_dict:
            return r.json()
        df = pd.DataFrame(r.json())
        df['curtailmentPeriodStart'] = pd.to_datetime(df['curtailmentPeriodStart'])\
                .dt.tz_convert('europe/amsterdam')
        df['curtailmentPeriodStop'] = pd.to_datetime(df['curtailmentPeriodStop'])\
                .dt.tz_convert('europe/amsterdam')
        return df

    def query_auction_bids_by_id(self, auction_id: str, as_dict: bool = False) -> pd.DataFrame | list:
        r = self.s.get(self.BASEURL + "getbids", params={
            'auctionid': auction_id
        })

        r.raise_for_status()

        if as_dict:
            return r.json()
        return pd.DataFrame(r.json())

    def query_auction_stats_months(self, month_from: date, month_to: date, corridor: str, horizon: str = 'Monthly') -> pd.DataFrame:
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
        :param horizon: string of a valid jao horizon from query_horizons
        :return:
        """
        detail_keys = ['bidGateOpening', 'bidGateClosure', 'offeredCapacity', 'atc',
                        'allocatedCapacity', 'resoldCapacity', 'requestedCapacity', 'auctionPrice']

        data = []
        m = month_from
        while m <= month_to:
            m_details = self.query_auction_details_by_month(corridor=corridor, month=m, horizon=horizon)
            m_data = {
                'id': m_details['identification'],
                'corridor': corridor,
                'month': m.replace(day=1)
            }
            m_data = {**m_data, **{k: v for k, v in m_details.items() if k in detail_keys}}
            data.append(m_data)
            m += relativedelta(months=1)
        df = pd.DataFrame(data)
        df = df['resoldCapacity'].fillna(0)
        df['nonAllocatedCapacity'] = df['offeredCapacity'] - df['allocatedCapacity']

        return df
