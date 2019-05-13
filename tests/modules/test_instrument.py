# import datetime
import uuid
import pytest

from Acquire.ObjectStore import ObjectStore
from Acquire.ObjectStore import string_to_encoded

from modules._instrument import Instrument
from objectstore import local_bucket

mocked_uuid = "00000000-0000-0000-00000-000000000000"

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
    # instrument = Instrument.create(name="test_name", site="test_site", 
    #                                 network="test_network", height="50m")

    assert instrument._name == "test_name"
    assert instrument._uuid == mocked_uuid
    assert instrument._site == "test_site"
    assert instrument._network == "test_network"
    assert instrument._height == "50m"


def test_creation_no_height(mock_uuid):
    instrument = Instrument.create(name="test_name", site="test_site",
                                   network="test_network")

    assert instrument._name == "test_name"
    assert instrument._uuid == mocked_uuid
    assert instrument._site == "test_site"
    assert instrument._network == "test_network"
    assert instrument._height is None


def test_to_data(mock_uuid, instrument):
    # instrument = Instrument.create(name="test_name", site="test_site",
    #                                network="test_network")
    data = instrument.to_data()

    assert data["name"] == "test_name"
    assert data["UUID"] == mocked_uuid
    assert data["site"] == "test_site"
    assert data["network"] == "test_network"
    assert data["height"] == "50m"

def test_from_data(mocked_uuid, instrument):
    data = instrument.to_data()

    new_instrument = Instrument.from_data(data)

    




    
