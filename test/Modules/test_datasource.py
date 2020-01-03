import datetime
import io
import os
from pathlib import Path
import pytest
import uuid
import numpy as np
import pandas as pd
import tempfile
import xarray
import zarr

from Acquire.ObjectStore import ObjectStore
from Acquire.ObjectStore import string_to_encoded

from HUGS.ObjectStore import get_dated_object
from HUGS.ObjectStore import get_dated_object_json
from HUGS.Modules import Datasource, CRDS
from HUGS.ObjectStore import get_local_bucket

mocked_uuid = "00000000-0000-0000-00000-000000000000"
mocked_uuid2 = "10000000-0000-0000-00000-000000000001"

@pytest.fixture(scope="session")
def data():
    filename = "bsd.picarro.1minute.248m.dat"
    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/CRDS"
    filepath = os.path.join(dir_path, test_data, filename)

    filepath = Path(filepath)

    crds = CRDS.load()
    combined_data = crds.read_data(data_filepath=filepath, site="bsd")
    
    return combined_data

@pytest.fixture
def datasource():
    return Datasource(name="test_name", )

@pytest.fixture
def mock_uuid(monkeypatch):

    def mock_uuid():
        return mocked_uuid

    monkeypatch.setattr(uuid, 'uuid4', mock_uuid)


@pytest.fixture
def mock_uuid2(monkeypatch):

    def mock_uuid():
        return mocked_uuid2

    monkeypatch.setattr(uuid, 'uuid4', mock_uuid)


def test_add_data(data):
    d = Datasource(name="test")

    metadata = data["ch4"]["metadata"]
    ch4_data = data["ch4"]["data"]

    # assert ch4_data["ch4 count"][0] == pytest.approx(1960.24)
    # assert ch4_data["ch4 stdev"][0] == pytest.approx(0.236)
    # assert ch4_data["ch4 n_meas"][0] == pytest.approx(26.0)

    d.add_data(metadata=metadata, data=ch4_data, data_type="CRDS")

    # assert d._data[0][0]["ch4 count"].equals(ch4_data["ch4 count"])
    # assert d._data[0][0]["ch4 stdev"].equals(ch4_data["ch4 stdev"])
    # assert d._data[0][0]["ch4 n_meas"].equals(ch4_data["ch4 n_meas"])

    # datasource_metadata = d.metadata()

    # assert datasource_metadata['data_type'] == 'timeseries'
    # assert datasource_metadata['height'] == '248m'
    # assert datasource_metadata['instrument'] == 'picarro'
    # assert datasource_metadata['port'] == '8'
    # assert datasource_metadata['site'] == 'bsd'
    # assert datasource_metadata['source_name'] == 'bsd.picarro.1minute.248m'
    # assert datasource_metadata['species'] == 'ch4'

def test_get_dataframe_daterange():
    n_days = 100
    epoch = datetime.datetime(1970,1,1,1,1)
    random_data = pd.DataFrame(data=np.random.randint(0, 100, size=(100, 4)), 
                    index=pd.date_range(epoch, epoch + datetime.timedelta(n_days-1), freq='D'), columns=list('ABCD'))

    d = Datasource(name="test")

    start, end = d.get_dataframe_daterange(random_data)

    assert start == pd.Timestamp("1970-01-01 01:01:00+0000")
    assert end == pd.Timestamp("1970-04-10 01:01:00+0000")


def test_save(mock_uuid2):
    bucket = get_local_bucket()

    datasource = Datasource(name="test_name")
    datasource.add_metadata(key="data_type", value="timeseries")
    datasource.save(bucket)

    prefix = "%s/uuid/%s" % (Datasource._datasource_root, datasource._uuid)

    objs = ObjectStore.get_all_object_names(bucket, prefix)

    assert objs[0].split("/")[-1] == mocked_uuid2

def test_save_footprint():
    bucket = get_local_bucket(empty=True)
    
    metadata = {"test": "testing123"}

    dir_path = os.path.dirname(__file__)
    test_data = "../data/emissions"
    filename = "WAO-20magl_EUROPE_201306_downsampled.nc"
    filepath = os.path.join(dir_path, test_data, filename)

    data = xarray.open_dataset(filepath)

    datasource = Datasource(name="test_name")
    datasource.add_footprint_data(metadata=metadata, data=data)
    datasource.save()

    prefix = "%s/uuid/%s" % (Datasource._datasource_root, datasource._uuid)
    objs = ObjectStore.get_all_object_names(bucket, prefix)

    datasource_2 = Datasource.load(bucket=bucket, key=objs[0])

    data = datasource_2._data[0][0]

    assert float(data.pressure[0].values) == pytest.approx(1023.971)
    assert float(data.pressure[2].values) == pytest.approx(1009.940)
    assert float(data.pressure[-1].values) == pytest.approx(1021.303)

def test_add_metadata(datasource):
    datasource.add_metadata(key="foo", value=123)
    datasource.add_metadata(key="bar", value=456)

    assert datasource._metadata["foo"] == "123"
    assert datasource._metadata["bar"] == "456"

def test_exists():
    d = Datasource(name="testing")
    d.save()

    exists = Datasource.exists(datasource_id=d.uuid())

    assert exists == True

def test_to_data(data):
    d = Datasource(name="testing_123")

    metadata = data["ch4"]["metadata"]
    ch4_data = data["ch4"]["data"]

    assert ch4_data["ch4 count"][0] == pytest.approx(1960.24)
    assert ch4_data["ch4 stdev"][0] == pytest.approx(0.236)
    assert ch4_data["ch4 n_meas"][0] == pytest.approx(26.0)

    d.add_data(metadata=metadata, data=ch4_data, data_type="CRDS")

    obj_data = d.to_data()

    metadata = obj_data["metadata"]
    assert obj_data["name"] == "testing_123"
    assert metadata["source_name"] == "bsd.picarro.1minute.248m"
    assert metadata["site"] == "bsd"
    assert metadata["instrument"] == "picarro"
    assert metadata["time_resolution"] == "1_minute"
    assert metadata["height"] == "248m"
    assert obj_data["data_type"] == "timeseries"
    assert len(obj_data["data_keys"]) == 0


def test_from_data(data):
    d = Datasource(name="testing_123")

    metadata = data["ch4"]["metadata"]
    ch4_data = data["ch4"]["data"]

    d.add_data(metadata=metadata, data=ch4_data, data_type="CRDS")

    obj_data = d.to_data()

    bucket = get_local_bucket()

    # Create a new object with the data from d
    d_2 = Datasource.from_data(bucket=bucket, data=obj_data, shallow=False)

    metadata = d_2.metadata()
    assert metadata["source_name"] == "bsd.picarro.1minute.248m"
    assert metadata["site"] == "bsd"
    assert metadata["instrument"] == "picarro"
    assert metadata["time_resolution"] == "1_minute"
    assert metadata["height"] == "248m"

    assert d_2.to_data() == d.to_data()

def test_update_daterange(data):
    metadata = {"foo": "bar"}

    d = Datasource(name="foo")
    
    ch4_data = data["ch4"]["data"]

    d.add_data(metadata=metadata, data=ch4_data)

    assert d._start_datetime == pd.Timestamp("2014-01-30 10:52:30+00:00")
    assert d._end_datetime == pd.Timestamp("2014-01-30 14:20:30+00:00")

    ch4_short = ch4_data.head(40)

    d.add_data(metadata=metadata, data=ch4_short, overwrite=True)

    assert d._start_datetime == pd.Timestamp("2014-01-30 10:52:30+00:00")
    assert d._end_datetime == pd.Timestamp("2014-01-30 13:22:30+00:00")
    
def test_load_dataset():
    filename = "WAO-20magl_EUROPE_201306_small.nc"
    dir_path = os.path.dirname(__file__)
    test_data = "../data/emissions"
    filepath = os.path.join(dir_path, test_data, filename)

    ds = xarray.load_dataset(filepath)

    metadata = {"some": "metadata"}

    d = Datasource("dataset_test")
    
    d.add_footprint_data(metadata=metadata, data=ds)

    d.save()

    key = list(d._data_keys.keys())[0]
    
    bucket = get_local_bucket()

    loaded_ds = Datasource.load_dataset(bucket=bucket, key=key)

    assert loaded_ds.equals(ds)

def test_search_metadata():
    d = Datasource(name="test_search")
    
    d._metadata = {"unladen": "swallow", "spam": "beans"}

    assert d.search_metadata("swallow") == True
    assert d.search_metadata("beans") == True
    assert d.search_metadata("BEANS") == True
    assert d.search_metadata("Swallow") == True

    assert d.search_metadata("eggs") == False
    assert d.search_metadata("flamingo") == False





    
