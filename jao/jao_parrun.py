from .jao import JaoPublicationToolPandasClient
import pandas as pd
from .parsers import parse_base_output
import warnings


class JaoPublicationToolPandasIntraDayParRun(JaoPublicationToolPandasClient):
    def __init__(self, version, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if version == 'c':
            self.BASEURL = "https://parallelrun-publicationtool.jao.eu/coreID/api/data/IDCCC_"
            warnings.warn("Parallel run of IDCC(c) is over, for production data use the normal client", DeprecationWarning)
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

    def query_monitoring(self, day: pd.Timestamp) -> list[dict]:
        # use this quick hack because this monitoring endpoint differs from all others
        base_url_old = self.BASEURL
        self.BASEURL = self.BASEURL.replace("IDCCC_", '')
        data = self._query_base_day(day, 'monitoring')
        self.BASEURL = base_url_old
        return data

    def query_fallbacks(self, day: pd.Timestamp) -> pd.DataFrame:
        return parse_base_output(
            self._query_base_day(day, 'fallbacks')
        ).drop(columns=['lastModifiedOn'])

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
