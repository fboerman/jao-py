from .jao import JaoPublicationToolClientBase
from .parsers import parse_final_domain, parse_base_output
import pandas as pd


class JaoPublicationToolItalyNorth(JaoPublicationToolClientBase):
    BASEURL = "https://publicationtool.jao.eu/ibwt/api/data/"

    def query_cnecs(
        self,
        mtu: pd.Timestamp,
        dayahead: bool = True,
        urls_only: bool = False
    ) -> list[dict]:
        return self._query_domain(
            "CCR_cnecInfo" if dayahead else "CCR_idCnecInfo",
            mtu=mtu,
            urls_only=urls_only,
        )

    def query_grid_forecasts(self,
                             d_from: pd.Timestamp,
                             d_to: pd.Timestamp,
                             dayahead: bool = True
                             ) -> list[dict]:
        return self._query_base_fromto(
            d_from=d_from, d_to=d_to,
            type='CCR_forecasted' if dayahead else "CCR_idForecasted"
        )

    def query_final_ntc_ttc(self,
                            d_from: pd.Timestamp,
                            d_to: pd.Timestamp,
                            dayahead: bool = True
                            ) -> list[dict]:
        return self._query_base_fromto(
            d_from=d_from, d_to=d_to,
            type='CCR_FinalTtcNtc' if dayahead else "CCR_idFinalTtcNtc"
        )

    def query_allocation_constraint(self,
                            d_from: pd.Timestamp,
                            d_to: pd.Timestamp,
                            dayahead: bool = True
                            ) -> list[dict]:
        return self._query_base_fromto(
            d_from=d_from, d_to=d_to,
            type='CCR_allocationConstraint' if dayahead else "CCR_idAllocationConstraint"
        )



class JaoPublicationToolPandasItalyNorth(JaoPublicationToolItalyNorth):
    def query_cnecs(
        self,
        mtu: pd.Timestamp,
        dayahead: bool = True
    ) -> pd.DataFrame:

        return parse_final_domain(
            super().query_cnecs(
                mtu=mtu,
                dayahead=dayahead
            )
        )

    def query_grid_forecasts(self,
                             d_from: pd.Timestamp,
                             d_to: pd.Timestamp,
                             dayahead: bool = True
                             ) -> pd.DataFrame:
        return parse_base_output(
            super().query_grid_forecasts(d_from=d_from, d_to=d_to, dayahead=dayahead)
        )

    def query_final_ntc_ttc(self,
                             d_from: pd.Timestamp,
                             d_to: pd.Timestamp,
                             dayahead: bool = True
                             ) -> pd.DataFrame:
        return parse_base_output(
            super().query_final_ntc_ttc(d_from=d_from, d_to=d_to, dayahead=dayahead)
        )

    def query_allocation_constraint(self,
                             d_from: pd.Timestamp,
                             d_to: pd.Timestamp,
                             dayahead: bool = True
                             ) -> pd.DataFrame:
        return parse_base_output(
            super().query_allocation_constraint(d_from=d_from, d_to=d_to, dayahead=dayahead)
        )
