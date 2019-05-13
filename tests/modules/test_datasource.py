import datetime
import uuid
import pytest

from Acquire.ObjectStore import ObjectStore
from Acquire.ObjectStore import string_to_encoded
# from objectstore.hugs_objstore import get_bucket

from modules import _datasource as Datasource
from objectstore import local_bucket

mocked_uuid = "00000000-0000-0000-00000-000000000000"

@pytest.fixture
def mock_uuid(monkeypatch):

    def mock_uuid():
        return mocked_uuid

    monkeypatch.setattr(uuid, 'uuid4', mock_uuid)


def test_creation(mock_uuid):
    datasource = Datasource.Datasource.create(name="test_name", instrument="test_instrument",
                                                site="test_site", network="test_network")

    assert datasource._name == "test_name"
    assert datasource._uuid == mocked_uuid
    assert datasource._instrument == "test_instrument"
    assert datasource._site == "test_site"
    assert datasource._network == "test_network"


def test_to_data(mock_uuid):
    datasource = Datasource.Datasource.create(name="test_name", instrument="test_instrument",
                                              site="test_site", network="test_network")

    data = datasource.to_data()

    assert data["UUID"] == mocked_uuid
    assert data["name"] == "test_name"
    assert data["instrument"] == "test_instrument"
    assert data["site"] == "test_site"
    assert data["network"] == "test_network"

    
def test_from_data(mock_uuid):
    datasource = Datasource.Datasource.create(name="test_name_two", instrument="test_instrument_two",
                                              site="test_site_two", network="test_network_two")

    new_datasource = Datasource.Datasource.from_data(datasource.to_data())

    assert new_datasource._name == "test_name_two"
    assert new_datasource._uuid == mocked_uuid
    assert new_datasource._instrument == "test_instrument_two"
    assert new_datasource._site == "test_site_two"
    assert new_datasource._network == "test_network_two"


def test_save(mock_uuid):
    datasource = Datasource.Datasource.create(name="test_name", instrument="test_instrument",
                                              site="test_site", network="test_network")

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

    
def test_load(mock_uuid):
    datasource = Datasource.Datasource.create(name="test_name_load", instrument="test_instrument_load",
                                              site="test_site_load", network="test_network_load")

    bucket = local_bucket.get_local_bucket()
    # Save to the HUGS bucket in the object store
    datasource.save(bucket)

    loaded_datasource = Datasource.Datasource.load(bucket=bucket, uuid=mocked_uuid)

    assert loaded_datasource._name == "test_name_load"
    assert loaded_datasource._uuid == mocked_uuid
    assert loaded_datasource._instrument == "test_instrument_load"
    assert loaded_datasource._site == "test_site_load"
    assert loaded_datasource._network == "test_network_load"


def test_get_uid_from_name(mock_uuid):
    name = 'test_name_getuid'
    datasource = Datasource.Datasource.create(name=name, instrument="test_instrument_load",
                                              site="test_site_load", network="test_network_load")

    from Acquire.ObjectStore import string_to_encoded as _string_to_encoded

    bucket = local_bucket.get_local_bucket()

    datasource.save(bucket)

    found_uuid = Datasource.Datasource._get_uid_from_name(bucket, name)

    assert found_uuid == mocked_uuid



    











                    





    
