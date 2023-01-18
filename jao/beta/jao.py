import requests
from ..parsers import parse_final_domain
import pandas as pd


class JaoPublicationToolNordicsPandasClient:
    ## WARNING: THIS IS STILL IN BETA
    # will be updated when the publication tool is improved and finalized over time
    BASEURL = "https://test-publicationtool.jao.eu/nordic/api/nordic/"

    def __init__(self):
        self.s = requests.Session()
        self.s.headers.update({
            'user-agent': 'jao-py (github.com/fboerman/jao-py)'
        })

    def _query_base(self, mtu, t):
        r = self.s.get(self.BASEURL + t + "/index", params={
            'date': mtu.tz_convert('UTC').strftime("%Y-%m-%dT%H:%MZ")
        })
        r.raise_for_status()

        data = r.json()
        if 'data' in data.keys():
            return r.json()['data']
        else:
            for _, v in data.items():
                if type(v) == list:
                    return v

    def query_final_domain(self, mtu):
        data = self._query_base(mtu, 'finalComputation')
        df = parse_final_domain(data)\
            .dropna(axis=1, how='all')\
            .drop(columns=['id_original'])
        df.loc[df['tso'] == '', 'tso'] = None
        df[['ram', 'imax', 'fmax', 'frm', 'fnrao', 'fref', 'fall', 'amr', 'aac', 'iva']] = \
            df[['ram', 'imax', 'fmax', 'frm', 'fnrao', 'fref', 'fall', 'amr', 'aac', 'iva']].astype(int)
        df[[x for x in df.columns if x.startswith('ptdf_')]] = df[[x for x in df.columns if x.startswith('ptdf_')]].fillna(0)
        return df

    def query_minmax(self, mtu):
        data = self._query_base(mtu, 'maxNetPos')
        # will always return the whole day
        df = pd.DataFrame(data)
        df['mtu'] = pd.to_datetime(df['dateTimeUtc'], utc=True).dt.tz_convert('europe/amsterdam')
        df = df.drop(columns=['dateTimeUtc', 'id']).set_index('mtu').sort_index()
        return df