import pandas as pd
from jao import JaoPublicationToolPandasIntraDay
import pytest


@pytest.fixture()
def mtu():
    mtu = pd.Timestamp('2026-10-14 18:00', tz='Europe/Amsterdam')
    yield mtu


@pytest.fixture(params=['b', 'c', 'd', 'e'], autouse=True)
def client(request):
    yield JaoPublicationToolPandasIntraDay(request.param)


def test_final_domain(client, mtu):
    df = client.query_final_domain(
        mtu=mtu,
        presolved=True
    )
    expected_values = {
        'b': 103,
        'c': 108,
        'd': 101,
        'e': 100
    }

    assert len(df) == expected_values[client.version]
