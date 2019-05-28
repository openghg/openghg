import datetime
import os
import pytest
import pandas as pd
import uuid

from Acquire.ObjectStore import datetime_to_string

from processing._metadata import Metadata

@pytest.fixture(scope="session")
def data():

    filename = "bsd.picarro.1minute.248m.dat"
    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/CRDS"
    filepath = os.path.join(dir_path, test_data, filename)

    return pd.read_csv(filepath, header=None, skiprows=1, sep=r"\s+")

@pytest.fixture(scope="session")
def metadata():
    return Metadata()


def test_parse_metadata(data):
    filename = "bsd.picarro.1minute.248m.dat"

    metadata = Metadata.create(filename=filename, data=data)

    start_datetime = datetime.datetime(2014, 1, 30, 10, 49, 30)
    end_datetime = datetime.datetime(2014, 1, 30, 14, 20, 30)

    assert metadata._data["site"] == "bsd"
    assert metadata._data["instrument"] == "picarro"
    assert metadata._data["resolution"] == "1m"
    assert metadata._data["height"] == "248m"
    assert metadata._data["start_datetime"] == datetime_to_string(start_datetime)
    assert metadata._data["end_datetime"] == datetime_to_string(end_datetime)
    assert metadata._data["port"] == "8"
    assert metadata._data["type"] == "air"


def test_parse_time(metadata):
    date = "190101"
    time = "153000"

    parsed_time = metadata.parse_date_time(date, time)
    correct_datetime = datetime.datetime(2019, 1, 1, 15, 30, 0)

    assert parsed_time == correct_datetime


def test_parse_filename(metadata):
    filename = "bsd.picarro.1minute.248m.dat"
    
    site, instrument, resolution, height = metadata.parse_filename(filename)

    assert site == "bsd"
    assert instrument == "picarro"
    assert resolution == "1m"
    assert height == "248m"
