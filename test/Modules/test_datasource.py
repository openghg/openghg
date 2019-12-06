import datetime
import os
from pathlib import Path
import pytest
import uuid
import numpy as np
import pandas as pd
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

def test_creation(mock_uuid, datasource):
    assert datasource._uuid == mocked_uuid
    assert datasource._name == "test_name"
    # assert datasource._instrument == "test_instrument"
    # assert datasource._site == "test_site"
    # assert datasource._network == "test_network"


def test_to_data(mock_uuid, datasource):
    data = datasource.to_data()

    assert data["UUID"] == mocked_uuid
    assert data["name"] == "test_name"
    # assert data["instrument"] == "test_instrument"
    # assert data["site"] == "test_site"
    # assert data["network"] == "test_network"

def test_from_data(mock_uuid):
    
    datasource = Datasource.create(name="test_name_two", instrument="test_instrument_two",
                                              site="test_site_two", network="test_network_two")

    bucket = get_local_bucket()

    data = datasource.to_data()
    new_datasource = Datasource.from_data(bucket=bucket, data=datasource.to_data(), shallow=False)

    assert new_datasource._name == "test_name_two"
    assert new_datasource._uuid == mocked_uuid




    
