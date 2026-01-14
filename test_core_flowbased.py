import pandas as pd
from jao import JaoPublicationToolPandasClient
import pytest


@pytest.fixture()
def client():
    client = JaoPublicationToolPandasClient()
    yield client


@pytest.fixture()
def mtu():
    mtu = pd.Timestamp('2025-03-23 12:00', tz='europe/amsterdam')
    yield mtu


def test_final_domain(client, mtu):
    df = client.query_final_domain(
        mtu=mtu,
        presolved=True
    )
    assert len(df) == 123


def test_prefinal_domain(client, mtu):
    df = client.query_prefinal_domain(
        mtu=mtu,
        presolved=True
    )
    assert len(df) == 119


def test_initial_domain(client, mtu):
    df = client.query_initial_domain(
        mtu=mtu
    )
    assert len(df) == 12430


def test_domains_with_tso(client, mtu):
    # Via TSO alias
    df = client.query_initial_domain(
        mtu=mtu,
        tso="TRANSNETBW",
    )
    assert len(df["tso"].unique()) == 1
    assert len(df) == 139

    # Via TSO EIC
    df = client.query_prefinal_domain(
        mtu=mtu,
        presolved=True,
        tso="10XDE-EON-NETZ-C",
    )
    assert len(df["tso"].unique()) == 1
    assert len(df) == 4

    # Multiple TSOs
    df = client.query_final_domain(
        mtu=mtu,
        presolved=True,
        tso=["TENNETBV", "10XDE-EON-NETZ-C"],
    )
    assert len(df["tso"].unique()) == 2
    assert len(df) == 12


def test_allocationconstraint(client, mtu):
    df = client.query_allocationconstraint(
        d_from=mtu,
        d_to=mtu + pd.Timedelta(hours=1)
    )
    assert len(df) == 1
    assert len(df.columns) == 4
    assert df.iloc[0].to_list() == [None, 0, None, 11987]


def test_monitoring(client, mtu):
    df = client.query_monitoring(mtu)
    assert len(df) == 28
    assert len(df.columns) == 6


def test_net_position(client, mtu):
    df = client.query_net_position(
        day=mtu
    )
    assert len(df) == 24
    assert len(df.columns) == 14


def test_active_constraints(client, mtu):
    df = client.query_active_constraints(
        day=mtu,
    )
    assert len(df) == 85
    assert len(df.columns) == 39


def test_maxbex(client, mtu):
    df = client.query_maxbex(
        day=mtu,
        from_zone='NL',
        to_zone='DE'
    )
    assert len(df) == 24
    assert list(df.columns) == ['NL>DE']


def test_minmax_np(client, mtu):
    df = client.query_minmax_np(
        day=mtu,
    )
    assert len(df) == 24
    assert len(df.columns) == 28


def test_lta(client, mtu):
    df = client.query_lta(
        d_from=mtu,
        d_to=mtu + pd.Timedelta(hours=1)
    )
    assert len(df) == 1
    assert len(df.columns) == 38


def test_validation(client):
    # take different mtu then other tests because there was no iva on selected hour
    mtu = pd.Timestamp('2023-03-23 14:00', tz='europe/amsterdam')
    df = client.query_validations(
        d_from=mtu,
        d_to=mtu + pd.Timedelta(hours=1)
    )
    assert len(df) == 13
    assert len(df.columns) == 29


def test_status(client, mtu):
    # take different mtu then other tests because there was no fallback in selected hour
    mtu = pd.Timestamp('2023-03-31 14:00', tz='europe/amsterdam')
    df = client.query_status(
        d_from=mtu,
        d_to=mtu + pd.Timedelta(hours=1)
    )
    assert len(df) == 3


def test_d2cf(client, mtu):
    df = client.query_d2cf(
        d_from=mtu,
        d_to=mtu + pd.Timedelta(hours=1)
    )
    assert len(df) == 1
    assert len(df.columns) == 51

def test_alpha_factor(client, mtu):
    # take different mtu then other tests because there was no iva on selected hour
    df = client.query_alpha_factor(d_from=mtu.replace(hour=0), d_to=mtu.replace(hour=23, minute=59))
    assert len(df) == 24

def test_price_spread(client, mtu):
    df = client.query_price_spread(
        d_from=mtu,
        d_to=mtu + pd.Timedelta(hours=1)
    )
    assert len(df) == 1
    assert len(df.columns) == 44


def test_scheduled_exchange(client, mtu):
    df = client.query_scheduled_exchange(
        d_from=mtu,
        d_to=mtu + pd.Timedelta(hours=1)
    )
    assert len(df) == 1
    assert len(df.columns) == 44


def test_refprog(client, mtu):
    df = client.query_refprog(
        d_from=mtu,
        d_to=mtu + pd.Timedelta(hours=1)
    )
    assert len(df) == 1
    assert len(df.columns) == 88
