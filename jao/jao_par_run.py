from .jao import JaoPublicationToolPandasClient
import pandas as pd
from typing import List, Dict
from .parsers import parse_base_output


class JaoPublicationToolPandasNordics(JaoPublicationToolPandasClient):
    BASEURL = "https://parallelrun-publicationtool.jao.eu/nordic/api/data/"

    # not implemented means that jao does not provide this endpoint for the nordics

    def query_lta(self, d_from: pd.Timestamp, d_to: pd.Timestamp):
        raise NotImplementedError

    def query_status(self, d_from: pd.Timestamp, d_to: pd.Timestamp):
        raise NotImplementedError

    def query_active_constraints(self, day: pd.Timestamp):
        raise NotImplementedError

    def query_allocationconstraint(self, d_from: pd.Timestamp, d_to: pd.Timestamp):
        raise NotImplementedError

    def query_net_position(self, day: pd.Timestamp):
        raise NotImplementedError


class JaoPublicationToolPandasIntraDay(JaoPublicationToolPandasClient):
    BASEURL = "https://parallelrun-publicationtool.jao.eu/coreID/api/data/ID2_"

    def query_lta(self, d_from: pd.Timestamp, d_to: pd.Timestamp):
        raise NotImplementedError

    def query_status(self, d_from: pd.Timestamp, d_to: pd.Timestamp):
        raise NotImplementedError

    def query_active_constraints(self, day: pd.Timestamp):
        raise NotImplementedError

    def query_sidc_atc_raw(self, day: pd.Timestamp) -> List[Dict]:
        return self._query_base_day(day, 'intradayAtc')

    def query_sidc_ntc_raw(self, day: pd.Timestamp) -> List[Dict]:
        return self._query_base_day(day, 'intradayNtc')

    def query_sidc_atc(self, day: pd.Timestamp, from_zone: str = None, to_zone: str = None) -> pd.DataFrame:
        df = parse_base_output(
            self.query_sidc_atc_raw(day=day)
        ).rename(columns=lambda x: x.lstrip('border_').replace('_', '>'))

        if from_zone is not None:
            df = df[[c for c in df.columns if c.split('>')[0] == from_zone]]

        if to_zone is not None:
            df = df[[c for c in df.columns if c.split('>')[1] == to_zone]]

        return df

    def query_sidc_ntc(self, day: pd.Timestamp, from_zone: str = None, to_zone: str = None) -> pd.DataFrame:
        df = parse_base_output(
            self.query_sidc_ntc_raw(day=day)
        ).rename(columns=lambda x: x.lstrip('border_').replace('_', '>'))

        if from_zone is not None:
            df = df[[c for c in df.columns if c.split('>')[0] == from_zone]]

        if to_zone is not None:
            df = df[[c for c in df.columns if c.split('>')[1] == to_zone]]

        return df
