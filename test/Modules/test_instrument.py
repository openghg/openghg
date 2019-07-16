# import datetime
import uuid
import pytest

from Acquire.ObjectStore import ObjectStore
from Acquire.ObjectStore import string_to_encoded

from HUGS.Modules import Instrument
from HUGS.ObjectStore import get_local_bucket

mocked_uuid = "10000000-0000-0000-00000-000000000001"

@pytest.fixture
def mock_uuid(monkeypatch):
    def mock_uuid():
        return mocked_uuid

    monkeypatch.setattr(uuid, 'uuid4', mock_uuid)

@pytest.fixture
def instrument(mock_uuid):
    return Instrument.create(name="test_name", site="test_site",
                             network="test_network", height="50m")


def test_creation(mock_uuid, instrument):
    assert instrument._name == "test_name"
    assert instrument._uuid == mocked_uuid
    assert instrument._labels["site"] == "test_site"
    assert instrument._labels["network"] == "test_network"
    assert instrument._labels["height"] == "50m"


def test_creation_no_height(mock_uuid):
    instrument = Instrument.create(name="test_name", site="test_site",
                                   network="test_network")

    assert instrument._name == "test_name"
    assert instrument._uuid == mocked_uuid
    assert instrument._labels["site"] == "test_site"
    assert instrument._labels["network"] == "test_network"
    
    with pytest.raises(KeyError):
        assert instrument._labels["height"] is None


def test_to_data(mock_uuid, instrument):
    data = instrument.to_data()

    assert data["name"] == "test_name"
    assert data["UUID"] == mocked_uuid
    assert data["labels"]["site"] == "test_site"
    assert data["labels"]["network"] == "test_network"
    assert data["labels"]["height"] == "50m"


def test_from_data(mock_uuid, instrument):
    data = instrument.to_data()
    new_instrument = Instrument.from_data(data, shallow=False)

    assert new_instrument._name == "test_name"
    assert new_instrument._uuid == mocked_uuid
    assert new_instrument._labels["site"] == "test_site"
    assert new_instrument._labels["network"] == "test_network"
    assert new_instrument._labels["height"] == "50m"


def test_save(instrument):
    bucket = get_local_bucket()

    instrument.save()

    instrument_key = "%s/uuid/%s" % (Instrument._instrument_root, mocked_uuid)

    data = ObjectStore.get_object_from_json(bucket=bucket, key=instrument_key)

    assert data["name"] == "test_name"
    assert data["UUID"] == mocked_uuid
    assert data["labels"]["site"] == "test_site"
    assert data["labels"]["network"] == "test_network"
    assert data["labels"]["height"] == "50m"


def test_load(instrument):
    bucket = get_local_bucket()
    instrument.save(bucket)

    loaded_instrument = Instrument.load(bucket=bucket, uuid=mocked_uuid)

    assert loaded_instrument._name == "test_name"
    assert loaded_instrument._uuid == mocked_uuid
    assert loaded_instrument._labels["site"] == "test_site"
    assert loaded_instrument._labels["network"] == "test_network"
    assert loaded_instrument._labels["height"] == "50m"


def test_get_uid_from_name(instrument):
    instrument = Instrument.create(name="test_name_two", site="test_site",
                      network="test_network", height="50m")

    instrument._uuid = "00000000-0000-1111-00000-000000000000"

    bucket = get_local_bucket()
    instrument.save(bucket)

    found_uuid = Instrument._get_uid_from_name(bucket, "test_name_two")

    assert found_uuid == "00000000-0000-1111-00000-000000000000"
    









    






    
