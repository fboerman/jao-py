import pandas as pd
from jao import JaoPublicationToolPandasItalyNorth
import pytest


@pytest.fixture()
def client():
    client = JaoPublicationToolPandasItalyNorth()
    yield client


@pytest.fixture()
def mtu():
    mtu = pd.Timestamp('2026-03-29 13:00', tz='Europe/Amsterdam')
    yield mtu


@pytest.fixture(params=[True, False], autouse=True)
def dayahead(request):
    return request.param


def test_query_cnecs(client, mtu, dayahead):
    df = client.query_cnecs(mtu=mtu, dayahead=dayahead)
    if dayahead:
        assert len(df) == 7628
    else:
        assert len(df) == 6960

def test_query_grid_forecasts(client, mtu, dayahead):
    df = client.query_grid_forecasts(
        d_from=mtu,
        d_to=mtu+pd.Timedelta(hours=1),
        dayahead=dayahead
    )
    assert len(df) == 1
    if dayahead:
        assert len(df.columns) == 19
    else:
        assert len(df.columns) == 15


def test_query_final_ntc_ttc(client, mtu, dayahead):
    df = client.query_final_ntc_ttc(
        d_from=mtu,
        d_to=mtu + pd.Timedelta(hours=1),
        dayahead=dayahead
    )
    assert len(df) == 4
    assert len(df.columns) == 11

def test_query_allocation_constraint(client, mtu, dayahead):
    df = client.query_allocation_constraint(
        d_from=mtu,
        d_to=mtu + pd.Timedelta(hours=1),
        dayahead=dayahead
    )
    assert len(df) == 1
    assert len(df.columns) == 5
