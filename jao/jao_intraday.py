from .jao import JaoPublicationToolPandasClient
import pandas as pd
from .parsers import parse_base_output
from typing import List, Dict


class JaoPublicationToolPandasIntraDay(JaoPublicationToolPandasClient):
    def __init__(self, version):
        super().__init__()
        if version == 'a':
            self.BASEURL = "https://publicationtool.jao.eu/coreID/api/data/IDCCA_"
        elif version == 'b':
            self.BASEURL = "https://publicationtool.jao.eu/coreID/api/data/IDCCB_"
        else:
            raise NotImplementedError

    def query_lta(self, d_from: pd.Timestamp, d_to: pd.Timestamp):
        raise NotImplementedError

    def query_status(self, d_from: pd.Timestamp, d_to: pd.Timestamp):
        raise NotImplementedError

    def query_active_constraints(self, day: pd.Timestamp):
        raise NotImplementedError

    def query_alpha_factor(self, d_from: pd.Timestamp, d_to: pd.Timestamp):
        raise NotImplementedError

    def query_monitoring(self, day: pd.Timestamp) -> List[Dict]:
        # use this quick hack because this monitoring endpoint differs from all others
        base_url_old = self.BASEURL
        self.BASEURL = self.BASEURL.replace("IDCCA_", '').replace("IDCCB_", '')
        data = self._query_base_day(day, 'monitoring')
        self.BASEURL = base_url_old
        return data

    def query_sidc_atc(self, day: pd.Timestamp, from_zone: str = None, to_zone: str = None) -> pd.DataFrame:
        df = parse_base_output(
            self._query_base_day(day, 'intradayAtc')
        ).rename(columns=lambda x: x.lstrip('border_').replace('_', '>'))

        if from_zone is not None:
            df = df[[c for c in df.columns if c.split('>')[0] == from_zone]]

        if to_zone is not None:
            df = df[[c for c in df.columns if c.split('>')[1] == to_zone]]

        return df

    def query_sidc_ntc(self, day: pd.Timestamp, from_zone: str = None, to_zone: str = None) -> pd.DataFrame:
        df = parse_base_output(
            self._query_base_day(day, 'intradayNtc')
        ).rename(columns=lambda x: x.lstrip('border_').replace('_', '>'))

        if from_zone is not None:
            df = df[[c for c in df.columns if c.split('>')[0] == from_zone]]

        if to_zone is not None:
            df = df[[c for c in df.columns if c.split('>')[1] == to_zone]]

        return df
