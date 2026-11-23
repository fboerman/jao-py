from datetime import date

import pandas as pd

from jao import JaoAPIClient


def test_resold_capacity_is_numeric_and_missing_values_are_zero():
    client = JaoAPIClient(api_key="not-a-real-key")
    resold = iter([None, 1.5, None])

    def fake_details(corridor, query_date, horizon, shadow_auctions_only=False):
        return {
            "identification": f"{corridor}-{query_date.isoformat()}",
            "offeredCapacity": 10.0,
            "allocatedCapacity": 8.0,
            "resoldCapacity": next(resold),
        }

    client.query_auction_details = fake_details

    df = client.query_auction_stats(date(2024, 4, 22), date(2024, 4, 24), "IF1-FR-GB", horizon="Daily")

    assert pd.api.types.is_float_dtype(df["resoldCapacity"])
    assert df["resoldCapacity"].tolist() == [0.0, 1.5, 0.0]
