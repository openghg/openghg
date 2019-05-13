import datetime
import os
import pytest
import pandas as pd
import uuid

from processing import _metadata as meta

@pytest.fixture(scope="session")
def data():

    filename = "bsd.picarro.1minute.248m.dat"
    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/CRDS"
    filepath = os.path.join(dir_path, test_data, filename)

    return pd.read_csv(filepath, header=None, skiprows=1, sep=r"\s+")

def test_parse_metadata(data):
    filename = "bsd.picarro.1minute.248m.dat"

    metadata = meta.parse_metadata(data=data, filename=filename)

    start_datetime = datetime.datetime(2014, 1, 30, 10, 49, 30)
    end_datetime = datetime.datetime(2014, 1, 30, 14, 20, 30)

    assert metadata["site"] == "bsd"
    assert metadata["instrument"] == "picarro"
    assert metadata["resolution"] == "1m"
    assert metadata["height"] == "248m"
    assert metadata["start_datetime"] == start_datetime
    assert metadata["end_datetime"] == end_datetime
    assert metadata["port"] == "8"
    assert metadata["type"] == "air"
