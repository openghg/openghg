import datetime
import numpy as np
import os
import pytest
import pandas as pd
import uuid

from processing import _segment as segment
from processing import _metadata as meta

from modules import Datasource
from objectstore import get_bucket
from Acquire.ObjectStore import ObjectStore

# from processing._segment import 

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

    monkeypatch.setattr(uuid, 'uuid4', mock_uuid)

    
@pytest.mark.slow
def test_get_split_frequency_large():

    date_range = pd.date_range("2010-01-01", "2019-01-01", freq="min")

    # Crates a ~ 1 GB dataframe
    df = pd.DataFrame(np.random.randint(0, 100, size=(len(date_range), 32)), index=date_range)

    split = segment.get_split_frequency(df)
    assert split == "W"


def test_get_split_frequency_small():
    date_range = pd.date_range("2010-01-01", "2019-01-01", freq="W")

    # Crates a small
    df = pd.DataFrame(np.random.randint(0, 100, size=(len(date_range), 32)), index=date_range)

    split = segment.get_split_frequency(df)
    assert split == "Y"


def test_get_datasources_correct_datetimes(data):
    datasource = segment.get_datasource(data)

    assert len(datasource._data) == 3
    assert datasource._start_datetime == pd.Timestamp("2014-01-30 10:52:30")
    assert datasource._end_datetime == pd.Timestamp("2014-01-30 14:20:30")
    

def test_get_datasource_already_exists(data):
    # Test get datasources when the Datasource object already exists
    uuid = "2e628682-094f-4ffb-949f-83e12e87a603"
    # Create a Datasource object and save it at key with this UUID
    d = Datasource.create(name="exists")
    d._uuid = uuid

    assert d._data is None

    bucket = get_bucket()
    datasource_key = "datasource/uuid/%s" % uuid
    ObjectStore.set_object_from_json(bucket=bucket, key=datasource_key, data=d.to_data())

    datasource = segment.get_datasource(data)

    assert datasource._uuid == uuid
    assert len(datasource._data) == 3
    assert datasource._start_datetime == pd.Timestamp("2014-01-30 10:52:30")
    assert datasource._end_datetime == pd.Timestamp("2014-01-30 14:20:30")


def test_column_naming(data):
    _, gas_data = segment.parse_gases(data)

    column_names = ["count", "stdev", "n_meas"]
    
    for gas_name, data in gas_data:
        # Check the name of each in the first dataframe
        for d in data:
            for i, col in enumerate(d.columns):
                assert column_names[i] in col


def test_parse_timecols(data):
    time_data = data.iloc[2:, 0:2]
    timeframe = segment.parse_timecols(time_data=time_data)

    assert timeframe.head(1)["Datetime"].iloc[0] == pd.to_datetime("2014-01-30 10:49:30")
    assert timeframe.tail(1)["Datetime"].iloc[0] == pd.to_datetime("2014-01-30 14:20:30")


def test_parse_gases_correct_data(data):
    _, gas_info = segment.parse_gases(data)

    # Unpack the list of tuples into two different tuples
    gas_names, gas_data = zip(*gas_info)

    assert sorted(gas_names) == sorted(['ch4', 'co', 'co2'])
    
    head_zero = gas_data[0][0].head(1)
    head_one = gas_data[1][0].head(1)
    head_two = gas_data[2][0].head(1)

    # Here iloc is index, column
    assert head_zero.first_valid_index() == pd.to_datetime("2014-01-30 10:52:30")
    assert head_zero.iloc[0, 0] == 1960.24
    assert head_zero.iloc[0, 1] == 0.236
    assert head_zero.iloc[0, 2] == 26.0

    assert head_one.first_valid_index() == pd.to_datetime("2014-01-30 10:52:30")
    assert head_one.iloc[0, 0] == 409.66
    assert head_one.iloc[0, 1] == 0.028
    assert head_one.iloc[0, 2] == 26.0

    assert head_two.first_valid_index() == pd.to_datetime("2014-01-30 10:52:30")
    assert head_two.iloc[0, 0] == 204.62
    assert head_two.iloc[0, 1] == 6.232
    assert head_two.iloc[0, 2] == 26.0

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

    










    
