from datetime import date, timedelta

import pytest
import requests

from jao import JaoAPIClient


class FakeResponse:
    def __init__(self, payload, status_code=200, text=""):
        self.payload = payload
        self.status_code = status_code
        self.reason = "OK" if status_code < 400 else ""
        self.text = text

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(self.reason)

    def json(self):
        return self.payload


def client_with(payload):
    client = JaoAPIClient(api_key="not-a-real-key")
    calls = []

    def fake_get(url, params):
        calls.append(params)
        return FakeResponse(payload)

    client.s.get = fake_get
    return client, calls


def test_range_is_split_into_month_windows_of_at_most_31_days():
    client, calls = client_with([])

    client.query_auctions("IF1-FR-GB", date(2024, 1, 1), date(2024, 12, 31), "Daily", as_dict=True)

    assert len(calls) == 12
    windows = [(date.fromisoformat(c["fromdate"]), date.fromisoformat(c["todate"])) for c in calls]
    assert windows[0] == (date(2023, 12, 31), date(2024, 1, 31))
    assert windows[1] == (date(2024, 1, 31), date(2024, 2, 29))
    assert windows[-1] == (date(2024, 11, 30), date(2024, 12, 31))
    assert all(to - frm <= timedelta(days=31) for frm, to in windows)
    # contiguous: every window starts where the previous one stopped
    assert all(a[1] == b[0] for a, b in zip(windows, windows[1:], strict=False))


def test_partial_months_at_both_ends():
    client, calls = client_with([])

    client.query_auctions("IF1-FR-GB", date(2024, 1, 15), date(2024, 3, 10), "Daily", as_dict=True)

    assert [(c["fromdate"], c["todate"]) for c in calls] == [
        ("2024-01-14", "2024-01-31"),
        ("2024-01-31", "2024-02-29"),
        ("2024-02-29", "2024-03-10"),
    ]


def test_one_row_per_product_with_results_matched_by_product_id():
    auction = {
        "identification": "IF1-FR-GB-D-DAILY--240402-01",
        "marketPeriodStart": "2024-04-01T22:00:00.000+00:00",
        "maintenances": [],
        "products": [
            {"productIdentification": "B21-------", "productHour": "20:00-21:00",
             "offeredCapacity": 1354.0, "comment": None},
            {"productIdentification": "B24-------", "productHour": "23:00-24:00",
             "offeredCapacity": 1300.0, "comment": None},
        ],
        # results arrive in a different order than products
        "results": [
            {"productIdentification": "B24-------", "productHour": "23:00-24:00",
             "allocatedCapacity": 1300.0, "auctionPrice": 25.79, "comment": "late"},
            {"productIdentification": "B21-------", "productHour": "20:00-21:00",
             "allocatedCapacity": 1354.0, "auctionPrice": 30.0, "comment": None},
        ],
    }
    client, calls = client_with([auction])

    df = client.query_auctions("IF1-FR-GB", date(2024, 4, 2), date(2024, 4, 2), "Daily")

    assert len(calls) == 1
    assert df["id"].tolist() == ["IF1-FR-GB-D-DAILY--240402-01"] * 2
    assert "maintenances" not in df.columns
    rows = df.set_index("productIdentification")
    assert rows.loc["B21-------", "auctionPrice"] == 30.0
    assert rows.loc["B24-------", "auctionPrice"] == 25.79
    assert rows.loc["B24-------", "resultComment"] == "late"


def test_auction_without_products_keeps_one_row():
    auction = {"identification": "X-D-DAILY--240402-01", "cancelled": True, "products": [], "results": []}
    client, _ = client_with([auction])

    df = client.query_auctions("X", date(2024, 4, 2), date(2024, 4, 2), "Daily")

    assert df["id"].tolist() == ["X-D-DAILY--240402-01"]
    assert df["cancelled"].tolist() == [True]


NO_DATA = '{"status":400,"message":"No Data found"}'


def test_windows_without_data_are_skipped():
    client = JaoAPIClient(api_key="not-a-real-key")
    yearly = {"identification": "IF1-FR-GB-Y-BASE---240101-01", "products": [], "results": []}
    responses = iter([FakeResponse([yearly])] + [FakeResponse(None, 400, NO_DATA)] * 11)
    client.s.get = lambda url, params: next(responses)

    df = client.query_auctions("IF1-FR-GB", date(2024, 1, 1), date(2024, 12, 31), "Yearly")

    assert df["id"].tolist() == ["IF1-FR-GB-Y-BASE---240101-01"]


def test_other_errors_are_raised_with_the_response_body():
    client = JaoAPIClient(api_key="not-a-real-key")
    body = '{"status":400,"message":"Max. time period of 31 days exceeded."}'
    client.s.get = lambda url, params: FakeResponse(None, 400, body)

    with pytest.raises(requests.HTTPError, match="31 days"):
        client.query_auctions("IF1-FR-GB", date(2024, 1, 1), date(2024, 1, 31), "Daily")
