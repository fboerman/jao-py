from .jao import JaoPublicationToolPandasClient
import pandas as pd

class JaoPublicationToolPandasIntraDayIda:
    def __init__(self, version: int, api_key: str = None, proxies: dict = None):
        self._client = JaoPublicationToolPandasClient(api_key=api_key, proxies=proxies)
        self._client.BASEURL = f"https://publicationtool.jao.eu/coreID/api/data/ID{version}_"

    def query_net_position(self, day: pd.Timestamp) -> pd.DataFrame:
        return self._client.query_net_position(day=day)

    def query_net_position_fromto(self, d_from: pd.Timestamp, d_to: pd.Timestamp) -> pd.DataFrame:
        return self._client.query_net_position_fromto(d_from=d_from, d_to=d_to)

    def query_scheduled_exchange(self, d_from: pd.Timestamp, d_to: pd.Timestamp) -> pd.DataFrame:
        return self._client.query_scheduled_exchange(d_from=d_from, d_to=d_to)

    def query_price_spread(self, d_from: pd.Timestamp, d_to: pd.Timestamp) -> pd.DataFrame:
        return self._client.query_price_spread(d_from=d_from, d_to=d_to)

    def query_congestion_income(self, d_from: pd.Timestamp, d_to: pd.Timestamp) -> pd.DataFrame:
        return self._client.query_congestion_income(d_from=d_from, d_to=d_to)
