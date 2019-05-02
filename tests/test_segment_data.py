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

# @pytest.fixture()
# def segmented_data():
#     filename = "bsd.picarro.1minute.248m.dat"
#     dir_path = os.path.dirname(__file__)
#     test_data = "data/proc_test_data/CRDS"
#     filepath = os.path.join(dir_path, test_data, filename)

#     return segment_data.parse_file(filepath=filepath)
    

def test_unanimous():

    true_dict = {"key1": 6, "key2": 6, "key3": 6}
    false_dict = {"key1": 3, "key2": 6, "key3": 9}

    assert segment_data.unanimous(true_dict) == True
    assert segment_data.unanimous(false_dict) == False

def test_parse_time():

    date = "190101"
    time = "153000"

    parsed_time = segment_data.parse_date_time(date, time)
    correct_datetime = datetime.datetime(2019, 1, 1, 15, 30, 0)

    assert parsed_time == correct_datetime

def test_parse_filename():

    filename = "bsd.picarro.1minute.248m.dat"

    site, instrument, resolution, height = segment_data.parse_filename(filename)

    assert site == "bsd"
    assert instrument == "picarro"
    assert resolution == "1m"
    assert height == "248m"


def test_gas_info(data):

    n_gases, n_cols = segment_data.gas_info(data=data)

    assert n_gases == 3
    assert n_cols == 3


def test_parse_metadata(data):

    filename = "bsd.picarro.1minute.248m.dat"

    metadata = segment_data.parse_metadata(data=data, filename=filename)

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


def test_parse_gases(data):

    header = data.head(2)
    # Count the number of columns before measurement data
    skip_cols = sum([header[column][0] == "-" for column in header.columns])

    gases = segment_data.parse_gases(data=data, skip_cols=4)

    assert sorted(gases.keys()) == ['ch4', 'co', 'co2']
    assert gases["ch4"]["data"][0][0] == "ch4"
    assert gases["co"]["data"][0][0] == "co"
    assert gases["co2"]["data"][0][0] == "co2"

    
def test_parse_file(monkeypatch):
    
    # TODO - look up adding creation of segmented data as a fixture
    # how to get this working with monkeypatch

    # Mock creation of the UUID
    fake_uuid = "00000000-0000-0000-00000-000000000000"
    def mock_uuid():
        return fake_uuid

    monkeypatch.setattr(uuid, 'uuid4', mock_uuid)

    filename = "bsd.picarro.1minute.248m.dat"
    dir_path = os.path.dirname(__file__)
    test_data = "data/proc_test_data/CRDS"
    filepath = os.path.join(dir_path, test_data, filename)

    gas_data = segment_data.parse_file(filepath=filepath)

    start_datetime = datetime.datetime(2014, 1, 30, 10, 49, 30)
    end_datetime = datetime.datetime(2014, 1, 30, 14, 20, 30)
    
    assert gas_data["metadata"]["site"] == "bsd"
    assert gas_data["metadata"]["instrument"] == "picarro"
    assert gas_data["metadata"]["resolution"] == "1m"
    assert gas_data["metadata"]["height"] == "248m"
    assert gas_data["metadata"]["start_datetime"] == start_datetime
    assert gas_data["metadata"]["end_datetime"] == end_datetime
    assert gas_data["metadata"]["port"] == "8"
    assert gas_data["metadata"]["type"] == "air"
    
    assert gas_data["metadata"]["gases"] == {"ch4": fake_uuid, "co": fake_uuid, "co2": fake_uuid}


def test_store_data():
    filename = "bsd.picarro.1minute.248m.dat"
    dir_path = os.path.dirname(__file__)
    test_data = "data/proc_test_data/CRDS"
    filepath = os.path.join(dir_path, test_data, filename)

    gases = segment_data.parse_file(filepath=filepath)

    # TODO

    
def test_key_creator(monkeypatch):
    # Mock creation of the UUID
    fake_uuid = "00000000-0000-0000-00000-000000000000"

    def mock_uuid():
        return fake_uuid

    monkeypatch.setattr(uuid, 'uuid4', mock_uuid)
    
    filename = "bsd.picarro.1minute.248m.dat"
    dir_path = os.path.dirname(__file__)
    test_data = "data/proc_test_data/CRDS"
    filepath = os.path.join(dir_path, test_data, filename)

    gases = segment_data.parse_file(filepath=filepath)

    metadata = gases["metadata"]

    key = segment_data.key_creator(metadata)

    metadata.pop("height", None)

    key_no_height = segment_data.key_creator(metadata)

    assert key == "bsd/picarro/248m/20140130_20140130/00000000-0000-0000-00000-000000000000"    
    assert key_no_height == "bsd/picarro/20140130_20140130/00000000-0000-0000-00000-000000000000"
    










    
