import os
import pytest
import uuid

from processing._crds import CRDS

mocked_uuid = "00000000-0000-0000-00000-000000000000"

@pytest.fixture
def mock_uuid(monkeypatch):
    def mock_uuid():
        return mocked_uuid
    monkeypatch.setattr(uuid, 'uuid4', mock_uuid)

def test_create(mock_uuid):
    filename = "bsd.picarro.1minute.248m.dat"
    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/CRDS"
    filepath = os.path.join(dir_path, test_data, filename)

    crds = CRDS.read_file(filepath)

    first_datetime = crds._datasources[0]._data["Datetime"][0]

    # TODO - check timestamp str and conversion to datetime
    
    assert crds._uuid == mocked_uuid
    # assert first_datetime == "2014-01-30 10:52:30"
    

