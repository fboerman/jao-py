from .jao import JaoPublicationToolPandasClient
import pandas as pd
from .parsers import parse_base_output


class JaoPublicationToolPandasIntraDay(JaoPublicationToolPandasClient):
    BASEURL_BARE = "https://publicationtool.jao.eu/coreID/api/data/"

    def __init__(self, version, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.version = version
        if version == 'a':
            self.BASEURL = self.BASEURL_BARE + "IDCCA_"
        elif version == 'b':
            self.BASEURL = self.BASEURL_BARE + "IDCCB_"
        elif version == 'c':
            self.BASEURL = self.BASEURL_BARE + "IDCCC_"
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
        self.BASEURL = self.BASEURL_BARE
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

    def query_fallbacks(self, day: pd.Timestamp) -> pd.DataFrame:
        if self.version == 'a':
            raise NotImplementedError
        return parse_base_output(
            self._query_base_day(day, 'fallbacks')
        ).drop(columns=['lastModifiedOn'])


    def query_validations_atc(self, day: pd.Timestamp) -> pd.DataFrame:
        if self.version == 'a':
            raise NotImplementedError
        return parse_base_output(
            self._query_base_day(day, 'validationReductionsATCs')
        )
