import pandas as pd
from jao import JaoPublicationToolPandasIntraDay
import pytest


@pytest.fixture()
def mtu():
    mtu = pd.Timestamp('2026-08-26 12:00', tz='Europe/Amsterdam')
    yield mtu


@pytest.fixture(params=['b', 'c', 'd'], autouse=True)
def client(request):
    yield JaoPublicationToolPandasIntraDay(request.param)


def test_final_domain(client, mtu):
    df = client.query_final_domain(
        mtu=mtu,
        presolved=True
    )
    expected_values = {
        'b': 107,
        'c': 116,
        'd': 116
    }

    assert len(df) == expected_values[client.version]
