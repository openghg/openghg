import datetime
import numpy as np
import os
import pytest
import pandas as pd
import uuid

from Acquire.ObjectStore import ObjectStore

from HUGS.Processing import get_split_frequency, get_datasources
from HUGS.Modules import Datasource
from HUGS.ObjectStore import get_bucket

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

    split = get_split_frequency(df)
    assert split == "W"


def test_get_split_frequency_small():
    date_range = pd.date_range("2010-01-01", "2019-01-01", freq="W")

    # Crates a small
    df = pd.DataFrame(np.random.randint(0, 100, size=(len(date_range), 32)), index=date_range)

    split = get_split_frequency(df)
    assert split == "Y"

def test_get_datasources_correct_number(data):
    datasources = get_datasources(data)
    
    assert len(datasources) == 3



# def test_get_datasources_correct_datetimes(data):
#     datasources = segment.get_datasources(data)
    
#     datasource = datasources[0]

#     # assert len(datasource._data) == 3
#     assert datasource._start_datetime == pd.Timestamp("2014-01-30 10:52:30")
#     assert datasource._end_datetime == pd.Timestamp("2014-01-30 14:20:30")
    

# def test_get_datasource_already_exists(data, mock_uuid):
#     # Test get datasources when the Datasource object already exists
#     uuid = "00000000-0000-1111-00000-000000000000"
#     # Create a Datasource object and save it at key with this UUID
#     d = Datasource.create(name="exists")
#     d._uuid = uuid

#     assert len(d._data) == 0

#     bucket = get_bucket()
#     datasource_key = "datasource/uuid/%s" % uuid
#     ObjectStore.set_object_from_json(bucket=bucket, key=datasource_key, data=d.to_data())

#     datasources = segment.get_datasources(data)

#     datasource = datasources[0]

#     assert datasource._uuid == uuid
#     assert len(datasource._data) == 1
#     assert datasource._start_datetime == pd.Timestamp("2014-01-30 10:52:30")
#     assert datasource._end_datetime == pd.Timestamp("2014-01-30 14:20:30")


# def test_column_naming(data):
#     gas_data = segment.parse_gases(data)

#     column_names = ["count", "stdev", "n_meas"]
    
#     for _, _, data in gas_data:
#         # Check the name of each in the first dataframe
#         for d in data:
#             for i, col in enumerate(d.columns):
#                 assert column_names[i] in col


# def test_parse_timecols(data):
#     time_data = data.iloc[2:, 0:2]
#     timeframe = segment.parse_timecols(time_data=time_data)

#     assert timeframe.head(1)["Datetime"].iloc[0] == pd.to_datetime("2014-01-30 10:49:30")
#     assert timeframe.tail(1)["Datetime"].iloc[0] == pd.to_datetime("2014-01-30 14:20:30")


# def test_parse_gases_correct_data(data):
#     gas_datas = segment.parse_gases(data)

#     # Unpack the list of tuples into two different tuples
#     gas_name, datasouce_uuids, gas_data = zip(*gas_datas)

#     # assert sorted(gas_names) == sorted(['ch4', 'co', 'co2'])
    
#     head_zero = gas_data[0][0].head(1)
#     head_one = gas_data[1][0].head(1)
#     head_two = gas_data[2][0].head(1)

#     # Here iloc is index, column
#     assert head_zero.first_valid_index() == pd.to_datetime("2014-01-30 10:52:30")
#     assert head_zero.iloc[0, 0] == 1960.24
#     assert head_zero.iloc[0, 1] == 0.236
#     assert head_zero.iloc[0, 2] == 26.0

#     assert head_one.first_valid_index() == pd.to_datetime("2014-01-30 10:52:30")
#     assert head_one.iloc[0, 0] == 409.66
#     assert head_one.iloc[0, 1] == 0.028
#     assert head_one.iloc[0, 2] == 26.0

#     assert head_two.first_valid_index() == pd.to_datetime("2014-01-30 10:52:30")
#     assert head_two.iloc[0, 0] == 204.62
#     assert head_two.iloc[0, 1] == 6.232
#     assert head_two.iloc[0, 2] == 26.0

# def test_unanimous():
#     true_dict = {"key1": 6, "key2": 6, "key3": 6}
#     false_dict = {"key1": 3, "key2": 6, "key3": 9}

#     assert segment.unanimous(true_dict) is True
#     assert segment.unanimous(false_dict) is False


# def test_gas_info(data):
#     n_gases, n_cols = segment.gas_info(data=data)

#     assert n_gases == 3
#     assert n_cols == 3









    
