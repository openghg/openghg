import datetime
import os
import pytest
import pandas as pd
import uuid

from processing import _segment as segment
from processing import _metadata as meta

# from processing._segment import 

@pytest.fixture(scope="session")
def data():

    filename = "bsd.picarro.1minute.248m.dat"
    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/CRDS"
    filepath = os.path.join(dir_path, test_data, filename)

    return pd.read_csv(filepath, header=None, skiprows=1, sep=r"\s+")

# @pytest.fixture()
# def segmented_data():
#     filename = "bsd.picarro.1minute.248m.dat"
#     dir_path = os.path.dirname(__file__)
#     test_data = "data/proc_test_data/CRDS"
#     filepath = os.path.join(dir_path, test_data, filename)

#     return segment.parse_file(filepath=filepath)
    

def test_get_datasources(data):
    # filename = "bsd.picarro.1minute.248m.dat"
    # dir_path = os.path.dirname(__file__)
    # test_data = "../data/proc_test_data/CRDS"
    # filepath = os.path.join(dir_path, test_data, filename)

    # data = pd.read_csv(filepath, header=None, skiprows=1, sep=r"\s+")
    
    datasources = segment.get_datasources(data)

    assert len(datasources) == 3

    assert datasources[0]._start_datetime == pd.Timestamp("2014-01-30 10:52:30")
    assert datasources[1]._start_datetime == pd.Timestamp("2014-01-30 10:52:30")
    assert datasources[2]._start_datetime == pd.Timestamp("2014-01-30 10:52:30")

    assert datasources[0]._end_datetime == pd.Timestamp("2014-01-30 14:20:30")
    assert datasources[1]._end_datetime == pd.Timestamp("2014-01-30 14:20:30")
    assert datasources[2]._end_datetime == pd.Timestamp("2014-01-30 14:20:30")


def test_parse_timecols(data):
    time_data = data.iloc[2:, 0:2]
    timeframe = segment.parse_timecols(time_data=time_data)

    assert timeframe.head(1)["Datetime"].iloc[0] == pd.to_datetime("2014-01-30 10:49:30")
    assert timeframe.tail(1)["Datetime"].iloc[0] == pd.to_datetime("2014-01-30 14:20:30")


def test_parse_gases(data):
    gas_info = segment.parse_gases(data)

    # Unpack the list of tuples into two lists 
    gas_names, gas_data = zip(*gas_info)

    assert sorted(gas_names) == sorted(['ch4', 'co', 'co2'])


def test_unanimous():
    true_dict = {"key1": 6, "key2": 6, "key3": 6}
    false_dict = {"key1": 3, "key2": 6, "key3": 9}

    assert segment.unanimous(true_dict) is True
    assert segment.unanimous(false_dict) is False


def test_gas_info(data):
    n_gases, n_cols = segment.gas_info(data=data)

    assert n_gases == 3
    assert n_cols == 3



# def test_parse_gases(data):
#     gases = segment.parse_gases(data=data)

#     assert sorted(gases.keys()) == ['ch4', 'co', 'co2']
#     assert gases["ch4"]["data"][0][0] == "ch4"
#     assert gases["co"]["data"][0][0] == "co"
#     assert gases["co2"]["data"][0][0] == "co2"

    
# def test_parse_file(monkeypatch):
    
#     # TODO - look up adding creation of segmented data as a fixture
#     # how to get this working with monkeypatch

#     # Mock creation of the UUID
#     fake_uuid = "00000000-0000-0000-00000-000000000000"
#     def mock_uuid():
#         return fake_uuid

#     monkeypatch.setattr(uuid, 'uuid4', mock_uuid)

#     filename = "bsd.picarro.1minute.248m.dat"
#     dir_path = os.path.dirname(__file__)
#     test_data = "../data/proc_test_data/CRDS"
#     filepath = os.path.join(dir_path, test_data, filename)

#     gas_data = segment.parse_file(filepath=filepath)

#     start_datetime = datetime.datetime(2014, 1, 30, 10, 49, 30)
#     end_datetime = datetime.datetime(2014, 1, 30, 14, 20, 30)
    
#     assert gas_data["metadata"]["site"] == "bsd"
#     assert gas_data["metadata"]["instrument"] == "picarro"
#     assert gas_data["metadata"]["resolution"] == "1m"
#     assert gas_data["metadata"]["height"] == "248m"
#     assert gas_data["metadata"]["start_datetime"] == start_datetime
#     assert gas_data["metadata"]["end_datetime"] == end_datetime
#     assert gas_data["metadata"]["port"] == "8"
#     assert gas_data["metadata"]["type"] == "air"
    
#     assert gas_data["metadata"]["gases"] == {"ch4": fake_uuid, "co": fake_uuid, "co2": fake_uuid}


# def test_store_data():
#     filename = "bsd.picarro.1minute.248m.dat"
#     dir_path = os.path.dirname(__file__)
#     test_data = "../data/proc_test_data/CRDS"
#     filepath = os.path.join(dir_path, test_data, filename)

#     gases = segment.parse_file(filepath=filepath)

#     # TODO

    










    
