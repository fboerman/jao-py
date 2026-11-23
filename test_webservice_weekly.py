from datetime import date, timedelta

from jao import JaoAPIClient


def test_weekly_stats_queried_weeks_are_contiguous_mondays():
    client = JaoAPIClient(api_key="not-a-real-key")
    queried = []

    def fake_details(corridor, query_date, horizon, shadow_auctions_only=False):
        queried.append(query_date)
        return {
            "identification": f"{corridor}-{query_date.isoformat()}",
            "offeredCapacity": 10.0,
            "allocatedCapacity": 8.0,
            "resoldCapacity": 1.5,
        }

    client.query_auction_details = fake_details

    date_from = date(2024, 4, 17)
    date_to = date(2024, 6, 30)
    df = client.query_auction_stats(date_from, date_to, "IF1-FR-GB", horizon="Weekly")

    expected = [date(2024, 4, 22) + timedelta(weeks=i) for i in range(10)]
    assert queried == expected
    assert df["date"].tolist() == expected
    assert all(q <= date_to for q in queried)
