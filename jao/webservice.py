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

    def query_auction_details(self, corridor: str, query_date: date, horizon: str, shadow_auctions_only: bool = False) -> dict:
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

        if horizon == 'Weekly':
            # weekly must start on monday
            query_date += relativedelta(weekday=0)

        if horizon == "Monthly":
            month_begin = query_date.replace(day=1)
            month_end = query_date.replace(day=monthrange(query_date.year, query_date.month)[1])
            r = self.s.get(self.BASEURL + 'getauctions', params={
                'corridor': corridor,
                'fromdate': (month_begin - timedelta(days=1)).strftime("%Y-%m-%d"),
                'horizon': horizon,
                'todate': month_end.strftime("%Y-%m-%d"),
                'shadow': int(shadow_auctions_only)
            })
        elif horizon == 'Yearly':
            r = self.s.get(self.BASEURL + 'getauctions', params={
                'corridor': corridor,
                'fromdate': f"{query_date.year-1}-12-31",
                'horizon': horizon,
                'shadow': int(shadow_auctions_only)
            })
        else:
            r = self.s.get(self.BASEURL + 'getauctions', params={
                'corridor': corridor,
                'fromdate': query_date.strftime("%Y-%m-%d-%H:%M:%S"),
                'horizon': horizon,
                'shadow': int(shadow_auctions_only)
            })

        # improves error feedback a lot
        if r.status_code >= 400 and not r.reason:
            r.reason = r.text
        r.raise_for_status()

        data = r.json()

        try:
            # prettify the results to only show the first products and results
            data = data[0]
            data = {**data, **data['results'][0], **data['products'][0]}
            del data['results']
            del data['products']
        except Exception:
            print(data)
            raise

        return data

    def query_auction_details_by_month(self, corridor: str, query_date: date, horizon="Monthly", shadow_auctions_only: bool = False) -> dict:
        return self.query_auction_details(corridor, query_date, horizon, shadow_auctions_only)

    def query_auctions(self, corridor: str, date_from: date, date_to: date, horizon: str,
                       shadow_auctions_only: bool = False, as_dict: bool = False) -> pd.DataFrame | list:
        """
        get all auctions whose delivery starts between date_from and date_to (both included)
        with all their products and results

        the range is requested in calendar-month windows, since the API answers at most 31 days
        per request: a year of daily auctions takes 12 requests instead of 365

        :param corridor: string of a valid jao corridor from query_corridors
        :param date_from: datetime.date object of the first delivery day
        :param date_to: datetime.date object of the last delivery day
        :param horizon: string of a valid jao horizon from query_horizons
        :param shadow_auctions_only: wether to only retrieve shadow auction results
        :param as_dict: return the raw auction list instead of a dataframe
        :return: dataframe with one row per auction product, results matched by productIdentification
        """
        auctions = []
        start = date_from
        while start <= date_to:
            stop = min(start.replace(day=monthrange(start.year, start.month)[1]), date_to)
            # the API selects on marketPeriodStart in UTC, which lies on the evening before
            # the first delivery day
            r = self.s.get(self.BASEURL + 'getauctions', params={
                'corridor': corridor,
                'fromdate': (start - timedelta(days=1)).strftime("%Y-%m-%d"),
                'todate': stop.strftime("%Y-%m-%d"),
                'horizon': horizon,
                'shadow': int(shadow_auctions_only)
            })
            start = stop + timedelta(days=1)
            # an empty window is answered with 400 instead of an empty list
            if r.status_code == 400 and 'No Data found' in r.text:
                continue
            if r.status_code >= 400 and not r.reason:
                r.reason = r.text
            r.raise_for_status()
            auctions.extend(r.json())

        if as_dict:
            return auctions

        rows = []
        for auction in auctions:
            base = {k: v for k, v in auction.items() if not isinstance(v, list)}
            base['id'] = base.pop('identification')
            results = {x['productIdentification']: x for x in auction['results']}
            for product in auction['products'] or [{}]:
                result = results.get(product.get('productIdentification'), {})
                rows.append({
                    **base,
                    **result,
                    **product,
                    'resultComment': result.get('comment'),
                })
        return pd.DataFrame(rows)

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
                .dt.tz_convert('Europe/Amsterdam')
        df['curtailmentPeriodStop'] = pd.to_datetime(df['curtailmentPeriodStop'])\
                .dt.tz_convert('Europe/Amsterdam')
        return df

    def query_auction_bids_by_id(self, auction_id: str, as_dict: bool = False) -> pd.DataFrame | list:
        r = self.s.get(self.BASEURL + "getbids", params={
            'auctionid': auction_id
        })

        r.raise_for_status()

        if as_dict:
            return r.json()
        return pd.DataFrame(r.json())

    def query_auction_stats(self, date_from: date, date_to: date, corridor: str, horizon: str = 'Monthly') -> pd.DataFrame:
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
        m = date_from
        if horizon == "Weekly":
            # query_auction_details moves weekly queries to the Monday of the
            # requested week; normalise here so every step advances one week
            m += relativedelta(weekday=0)
        while m <= date_to:
            m_details = self.query_auction_details(corridor=corridor, query_date=m, horizon=horizon)
            m_data = {
                'id': m_details['identification'],
                'corridor': corridor,
                'date': m
            }
            m_data = {**m_data, **{k: v for k, v in m_details.items() if k in detail_keys}}
            data.append(m_data)

            if horizon == "Weekly":
                m += relativedelta(weeks=1)
            elif horizon == "Daily":
                m += relativedelta(days=1)
            elif horizon == "Monthly":
                m += relativedelta(months=1)
            elif horizon == "Yearly":
                m += relativedelta(years=1)
            elif horizon == "Intraday":
                m += relativedelta(hours=1)
            else:
                m += relativedelta(days=7)
        df = pd.DataFrame(data)
        df['resoldCapacity'] = df['resoldCapacity'].astype(float).fillna(0)
        df['nonAllocatedCapacity'] = df['offeredCapacity'] - df['allocatedCapacity']

        return df

    def query_auction_stats_months(self, month_from: date, month_to: date, corridor: str, horizon: str = 'Monthly') -> pd.DataFrame:
        month_from = month_from.replace(day=1)

        return self.query_auction_stats(month_from, month_to, corridor, horizon)
