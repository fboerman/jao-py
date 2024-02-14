from .jao import JaoPublicationToolPandasClient
import pandas as pd


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
