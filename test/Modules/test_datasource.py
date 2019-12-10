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
    combined_data = crds.read_data(data_filepath=filepath)
    
    return combined_data

@pytest.fixture
def datasource():
    return Datasource.create(name="test_name", instrument="test_instrument",
                                site="test_site", network="test_network")

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
    d = Datasource.create(name="test") 

    metadata = data["ch4"]["metadata"]
    ch4_data = data["ch4"]["data"]

    assert ch4_data["ch4 count"].iloc[0] == pytest.approx(1960.24)
    assert ch4_data["ch4 stdev"].iloc[0] == pytest.approx(0.236)
    assert ch4_data["ch4 n_meas"].iloc[0] == pytest.approx(26.0)

    d.add_data(metadata=metadata, data=ch4_data, data_type="CRDS")

    assert d._data[0][0]["ch4 count"].iloc[0] == ch4_data["ch4 count"].iloc[0]
    assert d._data[0][0]["ch4 stdev"].iloc[0] == ch4_data["ch4 stdev"].iloc[0]
    assert d._data[0][0]["ch4 n_meas"].iloc[0] == ch4_data["ch4 n_meas"].iloc[0]

    datasource_metadata = d.metadata()

    assert datasource_metadata['data_type'] == 'timeseries'
    assert datasource_metadata['height'] == '248m'
    assert datasource_metadata['instrument'] == 'picarro'
    assert datasource_metadata['port'] == '8'
    assert datasource_metadata['site'] == 'bsd'
    assert datasource_metadata['source_name'] == 'test'
    assert datasource_metadata['species'] == 'ch4'

def test_get_dataframe_daterange():
    n_days = 100
    epoch = datetime.datetime(1970,1,1,1,1)
    random_data = pd.DataFrame(data=np.random.randint(0, 100, size=(100, 4)), 
                    index=pd.date_range(epoch, epoch + datetime.timedelta(n_days-1), freq='D'), columns=list('ABCD'))

    d = Datasource.create(name="test")

    start, end = d.get_dataframe_daterange(random_data)

    assert start == pd.Timestamp("1970-01-01 01:01:00+0000")
    assert end == pd.Timestamp("1970-04-10 01:01:00+0000")


def test_save(mock_uuid2):
    bucket = get_local_bucket()

    datasource = Datasource.create(name="test_name", instrument="test_instrument", site="test_site", network="test_network")
    datasource.add_metadata(key="data_type", value="timeseries")
    datasource.save(bucket)

    prefix = "%s/uuid/%s" % (Datasource._datasource_root, datasource._uuid)

    objs = ObjectStore.get_all_object_names(bucket, prefix)

    assert objs[0].split("/")[-1] == mocked_uuid2

def test_save_footprint():
    bucket = get_local_bucket(empty=True)
    
    metadata = {"test": "testing123"}

    dir_path = os.path.dirname(__file__)
    test_data = "../data"
    filename = "WAO-20magl_EUROPE_201306_downsampled.nc"
    filepath = os.path.join(dir_path, test_data, filename)

    data = xarray.open_dataset(filepath)

    datasource = Datasource.create(name="test_name")
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
    d = Datasource.create(name="testing")
    d.save()

    exists = Datasource.exists(datasource_id=d.uuid())

    assert exists == True

def test_to_data(data):
    d = Datasource.create(name="testing_123")

    metadata = data["ch4"]["metadata"]
    ch4_data = data["ch4"]["data"]

    assert ch4_data["ch4 count"].iloc[0] == pytest.approx(1960.24)
    assert ch4_data["ch4 stdev"].iloc[0] == pytest.approx(0.236)
    assert ch4_data["ch4 n_meas"].iloc[0] == pytest.approx(26.0)

    d.add_data(metadata=metadata, data=ch4_data, data_type="CRDS")

    obj_data = d.to_data()

    metadata = obj_data["metadata"]
    assert obj_data["name"] == "testing_123"
    assert metadata["source_name"] == "testing_123"
    assert metadata["site"] == "bsd"
    assert metadata["instrument"] == "picarro"
    assert metadata["time_resolution"] == "1_minute"
    assert metadata["height"] == "248m"
    assert obj_data["data_type"] == "timeseries"
    assert len(obj_data["data_keys"]) == 0


def test_from_data(data):
    d = Datasource.create(name="testing_123")

    metadata = data["ch4"]["metadata"]
    ch4_data = data["ch4"]["data"]

    d.add_data(metadata=metadata, data=ch4_data, data_type="CRDS")

    obj_data = d.to_data()

    bucket = get_local_bucket()

    # Create a new object with the data from d
    d_2 = Datasource.from_data(bucket=bucket, data=obj_data, shallow=False)

    metadata = d_2.metadata()
    assert metadata["source_name"] == "testing_123"
    assert metadata["site"] == "bsd"
    assert metadata["instrument"] == "picarro"
    assert metadata["time_resolution"] == "1_minute"
    assert metadata["height"] == "248m"

    assert d_2.to_data() == d.to_data()

def test_update_daterange():
    start = pd.Timestamp(year=2018, month=1, day=1, tz="UTC")
    end = pd.Timestamp(year=2019, month=1, day=1, tz="UTC")
    
    n_days = (end - start).days
    
    dated_data = pd.DataFrame(data=np.random.randint(0, 100, size=(n_days + 1, 4)), 
                                index=pd.date_range(start, end, freq='D'))

    d = Datasource.create(name="foo", data=dated_data)

    assert d._start_datetime == start
    assert d._end_datetime == end

    new_start = pd.Timestamp(year=2017, month=1, day=1, tz="UTC")
    new_end = pd.Timestamp(year=2019, month=12, day=31, tz="UTC")

    new_n_days = (new_end - new_start).days

    # Update the data
    updated_data = pd.DataFrame(data=np.random.randint(0, 100, size=(new_n_days + 1, 4)),
                              index=pd.date_range(new_start, new_end, freq='D'))

    metadata = {"foo": "bar"}

    d.add_data(metadata=metadata, data=updated_data)

    assert d._start_datetime == new_start
    assert d._end_datetime == new_end
    

def test_load_dataframe(data):
    d = Datasource.create(name="testing_123")

    metadata = data["ch4"]["metadata"]
    ch4_data = data["ch4"]["data"]
    
    d.add_data(metadata=metadata, data=ch4_data, data_type="CRDS")

    d.save()

    keys = list(d._data_keys.keys())

    bucket = get_local_bucket()

    df = Datasource.load_dataframe(bucket=bucket, key=keys[0])

    assert df["ch4 count"].iloc[0] == pytest.approx(1960.24)
    assert df["ch4 stdev"].iloc[0] == pytest.approx(0.236)
    assert df["ch4 n_meas"].iloc[0] == pytest.approx(26.0)

def test_dataframe_to_hdf():
    random_data = pd.DataFrame(data=np.random.randint(0, 100, size=(100, 4)), columns=list('ABCD'))

    hdf_data = Datasource.dataframe_to_hdf(data=random_data)

    df = Datasource.hdf_to_dataframe(hdf_data=hdf_data)

    assert df.equals(random_data)

def test_load_dataset():
    filename = "WAO-20magl_EUROPE_201306_small.nc"
    dir_path = os.path.dirname(__file__)
    test_data = "../data/emissions"
    filepath = os.path.join(dir_path, test_data, filename)

    ds = xarray.load_dataset(filepath)

    metadata = {"some": "metadata"}

    d = Datasource.create("dataset_test")
    d.add_footprint_data(metadata=metadata, data=ds)

    d.save()

    key = list(d._data_keys.keys())[0]
    
    bucket = get_local_bucket()

    loaded_ds = Datasource.load_dataset(bucket=bucket, key=key)

    assert loaded_ds.equals(ds)

def test_search_metadata():
    d = Datasource.create(name="test_search")
    
    d._metadata = {"unladen": "swallow", "spam": "beans"}

    assert d.search_metadata("swallow") == True
    assert d.search_metadata("beans") == True
    assert d.search_metadata("BEANS") == True
    assert d.search_metadata("Swallow") == True

    assert d.search_metadata("eggs") == False
    assert d.search_metadata("flamingo") == False





    
