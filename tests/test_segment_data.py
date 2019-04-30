import datetime
import os
import pytest
import pandas as pd
import uuid

import data_processing.segment_data as segment_data



@pytest.fixture(scope="session")
def data():
    filename = "bsd.picarro.1minute.248m.dat"
    dir_path = os.path.dirname(__file__)
    test_data = "data/proc_test_data/CRDS"
    filepath = os.path.join(dir_path, test_data, filename)

    return pd.read_csv(filepath, header=None, skiprows=1, sep=r"\s+")

def test_unanimous():
    true_dict = {"key1": 6, "key2": 6, "key3": 6}
    false_dict = {"key1": 3, "key2": 6, "key3": 9}

    assert segment_data.unanimous(true_dict) == True
    assert segment_data.unanimous(false_dict) == False

def test_parse_time():
    date = "190101"
    time = "153000"

    parsed_time = segment_data.parse_date_time(date, time)
    our_datetime = datetime.datetime(2019, 1, 1, 15, 30, 0)

    assert parsed_time == our_datetime

def test_find_gases(data):
    gases, n_cols = segment_data.find_gases(data)

    test_gases = {"co": 3, "co2": 3, "ch4": 3}
    
    assert gases == test_gases
    assert n_cols == 3


def test_parse_filename():
    filename = "bsd.picarro.1minute.248m.dat"

    site, instrument, resolution, height = segment_data.parse_filename(filename)

    assert site == "bsd"
    assert instrument == "picarro"
    assert resolution == "1m"
    assert height == "248m"


def test_parse_metadata(data):
    filename = "bsd.picarro.1minute.248m.dat"

    metadata = segment_data.parse_metadata(filename, data)

    start_datetime = datetime.datetime(2014, 1, 30, 10, 49, 30)
    end_datetime = datetime.datetime(2014, 1, 30, 14, 20, 30)

    gases = {"co": 3, "co2": 3, "ch4": 3}

    assert metadata["site"] == "bsd"
    assert metadata["instrument"] == "picarro"
    assert metadata["resolution"] == "1m"
    assert metadata["height"] == "248m"
    assert metadata["start_datetime"] == start_datetime
    assert metadata["end_datetime"] == end_datetime
    assert metadata["port"] == "8"
    assert metadata["type"] == "air"
    assert metadata["gases"] == gases



    












    
