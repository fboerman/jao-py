import requests
import pandas as pd
import json
from multiprocessing import Pool
import itertools
from .exceptions import NoMatchingDataError
from .parsers import parse_final_domain, parse_base_output
from typing import List, Dict
from .util import to_snake_case

__title__ = "jao-py"
__version__ = "0.4.5"
__author__ = "Frank Boerman"
__license__ = "MIT"


class JaoPublicationToolClient:
    BASEURL = "https://publicationtool.jao.eu/core/api/data/"

    def __init__(self, api_key: str = None):
        self.s = requests.Session()
        self.s.headers.update({
            'user-agent': 'jao-py (github.com/fboerman/jao-py)'
        })

        if api_key is not None:
            self.s.headers.update({
                'Authorization': 'Bearer ' + api_key
            })

    def _starmap_pull(self, url, params, keyname=None):
        r = self.s.get(url, params=params)
        r.raise_for_status()
        if keyname is not None:
            return r.json()[keyname]
        else:
            return r.json()

    def query_final_domain(self, mtu: pd.Timestamp, presolved: bool = None, cne: str = None, co: str = None,
                           urls_only: bool = False) -> List[Dict]:
        if type(mtu) != pd.Timestamp:
            raise Exception('Please use a timezoned pandas Timestamp object for mtu')
        if mtu.tzinfo is None:
            raise Exception('Please use a timezoned pandas Timestamp object for mtu')
        mtu = mtu.tz_convert('UTC')
        if cne is not None or co is not None or bool is not None:
            filter = {
                'CnecName': "" if cne is None else cne,
                'Contingency': "" if co is None else co,
                'Presolved': presolved
            }
        else:
            filter = None

        # first do a call with zero retrieved data to know how much data is available, then pull all at once
        r = self.s.get(self.BASEURL + "finalComputation", params={
            'FromUtc': mtu.isoformat(),
            'ToUtc': (mtu + pd.Timedelta(hours=1)).isoformat(),
            'Filter': json.dumps(filter),
            'Skip': 0,
            'Take': 0
        })
        r.raise_for_status()

        if r.json()['totalRowsWithFilter'] == 0:
            raise NoMatchingDataError

        # now do new call with all data requested
        # jao servers are not great returning it all at once, but they let you choose your own pagination
        # lets go for chunks of 5000, arbitrarily chosen

        total_num_data = r.json()['totalRowsWithFilter']
        args = []
        for i in range(0, total_num_data, 5000):
            args.append((self.BASEURL + "finalComputation", {
                'FromUtc': mtu.isoformat(),
                'ToUtc': (mtu + pd.Timedelta(hours=1)).isoformat(),
                'Filter': json.dumps(filter),
                'Skip': i,
                'Take': 5000
            }, 'data'))

        if urls_only:
            return args

        with Pool() as pool:
            results = pool.starmap(self._starmap_pull, args)

        return list(itertools.chain(*results))

    def _query_base_fromto(self, d_from: pd.Timestamp, d_to: pd.Timestamp, type: str) -> List[Dict]:
        r = self.s.get(self.BASEURL + type, params={
            'FromUTC': d_from.isoformat(),
            'ToUTC': d_to.isoformat()
        })
        r.raise_for_status()
        data = r.json()['data']
        if len(data) == 0:
            raise NoMatchingDataError
        return data

    def _query_base_day(self, day: pd.Timestamp, type: str) -> List[Dict]:
        d_from = day.replace(hour=0, minute=0)
        d_to = day.replace(hour=0, minute=0) + pd.Timedelta(days=1)
        # the api does some funny stuff on dst days, so adjust the to for this edge case
        if d_from.dst() > d_to.dst():
            d_to += pd.Timedelta(hours=1)
        elif d_from.dst() < d_to.dst():
            d_to -= pd.Timedelta(hours=1)
        return self._query_base_fromto(
            d_from=d_from,
            d_to=d_to,
            type=type
        )

    def query_net_position(self, day: pd.Timestamp) -> List[Dict]:
        return self._query_base_day(
            day=day,
            type='netPos'
        )

    def query_active_constraints(self, day: pd.Timestamp) -> List[Dict]:
        # although the same skip/take mechanism is active on this endpoint as the final domain, this is not needed to be used
        #   by definition active constraints are only a few so its overkill to start pagination
        # for the same reason this endpoint returns a whole day at once instead of per hour since there are not many
        #  and you probably want the whole day anyway
        # for the date range to be correct make sure the day input has a timezone!

        return self._query_base_day(
            day=day,
            type='shadowPrices'
        )

    def query_lta(self, d_from: pd.Timestamp, d_to: pd.Timestamp) -> List[Dict]:
        return self._query_base_fromto(d_from, d_to, 'lta')

    def query_validations(self, d_from: pd.Timestamp, d_to: pd.Timestamp) -> List[Dict]:
        return self._query_base_fromto(d_from, d_to, 'validationReductions')

    def query_maxbex(self, day: pd.Timestamp) -> List[Dict]:
        return self._query_base_day(day, 'maxExchanges')

    def query_minmax_np(self, day: pd.Timestamp) -> List[Dict]:
        return self._query_base_day(day, 'maxNetPos')

    def query_allocationconstraint(self, d_from: pd.Timestamp, d_to: pd.Timestamp) -> List[Dict]:
        return self._query_base_fromto(d_from, d_to, 'allocationConstraint')

    def query_status(self, d_from: pd.Timestamp, d_to: pd.Timestamp) -> List[Dict]:
        return self._query_base_fromto(d_from, d_to, 'spanningDefaultFBP')

    def query_price_spread(self, d_from: pd.Timestamp, d_to: pd.Timestamp) -> List[Dict]:
        return self._query_base_fromto(d_from, d_to, 'priceSpread')

    def query_scheduled_exchange(self, d_from: pd.Timestamp, d_to: pd.Timestamp) -> List[Dict]:
        return self._query_base_fromto(d_from, d_to, 'scheduledExchanges')


class JaoPublicationToolPandasClient(JaoPublicationToolClient):
    def query_final_domain(self, mtu: pd.Timestamp, presolved: bool = None, cne: str = None,
                           co: str = None) -> pd.DataFrame:
        return parse_final_domain(
            super().query_final_domain(mtu=mtu, presolved=presolved, cne=cne, co=co)
        )

    def query_allocationconstraint(self, d_from: pd.Timestamp, d_to: pd.Timestamp) -> pd.DataFrame:
        return parse_base_output(
            super().query_allocationconstraint(d_from=d_from, d_to=d_to)
        ).rename(columns=lambda c: c.split('_')[1] + '_' + ('import' if 'Down' in c.split('_')[0] else 'export'))

    def query_net_position(self, day: pd.Timestamp) -> pd.DataFrame:
        return parse_base_output(
            super().query_net_position(day=day)
        ).rename(columns=lambda x: x.replace('hub_', '')) \
            .rename(columns={'DE': 'DE_LU'})

    def query_active_constraints(self, day: pd.Timestamp) -> pd.DataFrame:
        return parse_base_output(
            super().query_active_constraints(day=day)
        ).rename(columns=lambda x: to_snake_case(x) if 'hub' not in x else x) \
            .rename(columns={'id': 'id_original'}) \
            .rename(columns=lambda x: x.replace('hub_', 'ptdf_'))

    def query_maxbex(self, day: pd.Timestamp, from_zone: str = None, to_zone: str = None) -> pd.DataFrame:
        df = parse_base_output(
            super().query_maxbex(day=day)
        ).rename(columns=lambda x: x.lstrip('border_').replace('_', '>'))

        if from_zone is not None:
            df = df[[c for c in df.columns if c.split('>')[0] == from_zone]]

        if to_zone is not None:
            df = df[[c for c in df.columns if c.split('>')[1] == to_zone]]

        return df

    def query_minmax_np(self, day: pd.Timestamp) -> pd.DataFrame:
        return parse_base_output(
            super().query_minmax_np(day=day)
        )

    def query_lta(self, d_from: pd.Timestamp, d_to: pd.Timestamp) -> pd.DataFrame:
        return parse_base_output(
            super().query_lta(d_from=d_from, d_to=d_to)
        )

    def query_validations(self, d_from: pd.Timestamp, d_to: pd.Timestamp) -> pd.DataFrame:
        df = parse_base_output(
            super().query_validations(d_from=d_from, d_to=d_to)
        ).rename(columns=to_snake_case)
        return df

    def query_status(self, d_from: pd.Timestamp, d_to: pd.Timestamp) -> pd.DataFrame:
        return parse_base_output(
            super().query_status(d_from=d_from, d_to=d_to)
        ).drop(columns=['lastModifiedOn'])

    def query_price_spread(self, d_from: pd.Timestamp, datetime, d_to: pd.Timestamp) -> (
            pd.DataFrame):
        return parse_base_output(
            super().query_price_spread(d_from=d_from, d_to=d_to)
        )

    def query_scheduled_exchange(self, d_from: pd.Timestamp, d_to: pd.Timestamp) -> (
            pd.DataFrame):
        return parse_base_output(
            super().query_scheduled_exchange(d_from=d_from, d_to=d_to)
        )
