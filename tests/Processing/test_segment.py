import os
import uuid

import numpy as np
import pandas as pd
import pytest

from HUGS.Processing import get_split_frequency

mocked_uuid = "00000000-0000-1111-00000-000000000000"


@pytest.fixture(scope="session")
def data():

    filename = "bsd.picarro.1minute.248m.dat"
    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/CRDS"
    filepath = os.path.join(dir_path, test_data, filename)

    return pd.read_csv(filepath, header=None, skiprows=1, sep=r"\s+")


@pytest.fixture
def mock_uuid(monkeypatch):
    def mock_uuid():
        return mocked_uuid

    monkeypatch.setattr(uuid, "uuid4", mock_uuid)


@pytest.mark.slow
def test_get_split_frequency_large():

    date_range = pd.date_range("2010-01-01", "2019-01-01", freq="min")

    # Crates an ~ 1 GB dataframe
    df = pd.DataFrame(
        np.random.randint(0, 100, size=(len(date_range), 32)), index=date_range
    )

    split = get_split_frequency(df)
    assert split == "W"


def test_get_split_frequency_small():
    date_range = pd.date_range("2010-01-01", "2019-01-01", freq="W")

    # Crates a small
    df = pd.DataFrame(
        np.random.randint(0, 100, size=(len(date_range), 32)), index=date_range
    )

    split = get_split_frequency(df)
    assert split == "Y"
