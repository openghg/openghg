import datetime
import os
import pytest
import uuid
import pandas as pd

from Acquire.ObjectStore import ObjectStore
from Acquire.ObjectStore import string_to_encoded

from modules._datasource import Datasource
from processing._metadata import Metadata
from objectstore import local_bucket

mocked_uuid = "00000000-0000-0000-00000-000000000000"


@pytest.fixture(scope="session")
def data():

    filename = "bsd.picarro.1minute.248m.dat"
    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/CRDS"
    filepath = os.path.join(dir_path, test_data, filename)

    return pd.read_csv(filepath, header=None, skiprows=1, sep=r"\s+")

@pytest.fixture
def datasource():
    return Datasource.create(name="test_name", instrument="test_instrument",
                                site="test_site", network="test_network")

@pytest.fixture
def mock_uuid(monkeypatch):

    def mock_uuid():
        return mocked_uuid

    monkeypatch.setattr(uuid, 'uuid4', mock_uuid)


def test_creation(mock_uuid, datasource):
    assert datasource._name == "test_name"
    assert datasource._uuid == mocked_uuid
    assert datasource._instrument == "test_instrument"
    assert datasource._site == "test_site"
    assert datasource._network == "test_network"


def test_to_data(mock_uuid, datasource):
    data = datasource.to_data()

    assert data["UUID"] == mocked_uuid
    assert data["name"] == "test_name"
    assert data["instrument"] == "test_instrument"
    assert data["site"] == "test_site"
    assert data["network"] == "test_network"

def test_save(mock_uuid, datasource):
    bucket = local_bucket.get_local_bucket()
    # Save to the HUGS bucket in the object store
    datasource.save(bucket)

    datasource_key = "%s/uuid/%s" % (datasource._datasource_root, mocked_uuid)

    data = ObjectStore.get_object_from_json(bucket=bucket, key=datasource_key)

    assert data["UUID"] == mocked_uuid
    assert data["name"] == "test_name"
    assert data["instrument"] == "test_instrument"
    assert data["site"] == "test_site"
    assert data["network"] == "test_network"

def test_save_with_data(mock_uuid, data):
    from processing._segment import get_datasources

    bucket = local_bucket.get_local_bucket()
    
    datasource = get_datasources(data=data)[0]

    original_slice = datasource._data.head(1)

    data = Datasource.create(name="test", instrument="test_instrument", site="site", 
                                network="test_network", data=datasource._data)

    data.save(bucket)

    new_datasource = Datasource.load(bucket, name="test")

    new_slice = new_datasource._data.head(1)

    assert new_slice.equals(original_slice)
    
def test_from_data(mock_uuid):
    
    datasource = Datasource.create(name="test_name_two", instrument="test_instrument_two",
                                              site="test_site_two", network="test_network_two")

    bucket = local_bucket.get_local_bucket()

    data = datasource.to_data()
    new_datasource = Datasource.from_data(bucket=bucket, data=datasource.to_data())

    assert new_datasource._name == "test_name_two"
    assert new_datasource._uuid == mocked_uuid
    assert new_datasource._instrument == "test_instrument_two"
    assert new_datasource._site == "test_site_two"
    assert new_datasource._network == "test_network_two"



    
def test_load(mock_uuid, datasource):
    bucket = local_bucket.get_local_bucket()
    # Save to the HUGS bucket in the object store
    datasource.save(bucket)

    loaded_datasource = Datasource.load(bucket=bucket, uuid=mocked_uuid)

    assert loaded_datasource._name == "test_name"
    assert loaded_datasource._uuid == mocked_uuid
    assert loaded_datasource._instrument == "test_instrument"
    assert loaded_datasource._site == "test_site"
    assert loaded_datasource._network == "test_network"


def test_get_uid_from_name(mock_uuid):
    from Acquire.ObjectStore import string_to_encoded

    name = 'test_name_getuid'
    datasource = Datasource.create(name=name, instrument="test_instrument_load",
                                              site="test_site_load", network="test_network_load")


    bucket = local_bucket.get_local_bucket()

    datasource.save(bucket)

    found_uuid = Datasource._get_uid_from_name(bucket, name)

    assert found_uuid == mocked_uuid

 





    
