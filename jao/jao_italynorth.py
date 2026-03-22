from .jao import JaoPublicationToolClientBase
from .parsers import parse_final_domain
import pandas as pd


class JaoPublicationToolItalyNorth(JaoPublicationToolClientBase):
    BASEURL = "https://publicationtool.jao.eu/ibwt/api/data/"

    def query_cnecs_dayahead(
        self,
        mtu: pd.Timestamp,
        presolved: bool = None,
        cne: str = None,
        co: str = None,
        tso: str | list[str] | None = None,
        urls_only: bool = False
    ) -> list[dict]:
        return self._query_domain(
            "CCR_cnecInfo",
            mtu=mtu,
            presolved=presolved,
            cne=cne,
            co=co,
            tso=tso,
            urls_only=urls_only,
        )

class JaoPublicationToolPandasItalyNorth(JaoPublicationToolItalyNorth):
    def query_cnecs_dayahead(
        self,
        mtu: pd.Timestamp,
        presolved: bool = None,
        cne: str = None,
        co: str = None,
        tso: str | list[str] | None = None
    ) -> pd.DataFrame:

        return parse_final_domain(
            super().query_cnecs_dayahead(
                mtu=mtu, presolved=presolved, cne=cne, co=co, tso=tso
            )
        )
