from .jao import JaoPublicationToolPandasClient
import pandas as pd
from .parsers import parse_final_domain


class JaoPublicationToolPandasNordics(JaoPublicationToolPandasClient):
    BASEURL = "https://publicationtool.jao.eu/nordic/api/data/"

    def query_allocationconstraint(self, d_from: pd.Timestamp, d_to: pd.Timestamp) -> pd.DataFrame:
        raise NotImplementedError

    def query_net_position(self, day: pd.Timestamp) -> pd.DataFrame:
        raise NotImplementedError

    def query_lta(self, d_from: pd.Timestamp, d_to: pd.Timestamp) -> pd.DataFrame:
        raise NotImplementedError

    def query_alpha_factor(self, d_from: pd.Timestamp, d_to: pd.Timestamp) -> pd.DataFrame:
        raise NotImplementedError

    def query_price_spread(self, d_from: pd.Timestamp, datetime, d_to: pd.Timestamp) -> (
            pd.DataFrame):
        raise NotImplementedError

    def query_scheduled_exchange(self, d_from: pd.Timestamp, d_to: pd.Timestamp) -> (
            pd.DataFrame):
        raise NotImplementedError

    def query_active_constraints(self, mtu: pd.Timestamp, shadow_price_only: bool = False) -> pd.DataFrame:
        df = parse_final_domain(
            super()._query_domain('fbDomainShadowPrice', mtu=mtu)
        )

        if shadow_price_only:
            df = pd.DataFrame(df[~pd.isna(df['shadow_price'])])

        return df
