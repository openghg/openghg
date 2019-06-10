import datetime
import os
import pytest
import uuid

from modules import CRDS
from objectstore import get_local_bucket
from processing import Metadata

from Acquire.ObjectStore import string_to_datetime
from Acquire.ObjectStore import datetime_to_datetime

# @pytest.fixture(scope="session")
# def data():
#     filename = "bsd.picarro.1minute.248m.dat"
#     dir_path = os.path.dirname(__file__)
#     test_data = "../data/proc_test_data/CRDS"
#     filepath = os.path.join(dir_path, test_data, filename)

#     return pd.read_csv(filepath, header=None, skiprows=1, sep=r"\s+")

mocked_uuid = "10000000-0000-0000-00000-000000000001"

@pytest.fixture
def mock_uuid(monkeypatch):
    def mock_uuid():
        return mocked_uuid

    monkeypatch.setattr(uuid, 'uuid4', mock_uuid)

@pytest.fixture
def crds(mock_uuid):
    filename = "bsd.picarro.1minute.248m.dat"
    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/CRDS"
    filepath = os.path.join(dir_path, test_data, filename)

    return CRDS.read_file(filepath=filepath)

def test_read_file(crds):
    start = datetime.datetime(2014,1,30,10,52,30)
    end = datetime.datetime(2014, 1, 30, 14, 20, 30)

    assert crds._start_datetime == start
    assert crds._end_datetime == end
    assert crds._uuid == mocked_uuid


def test_to_data(crds):
    crds_dict = crds.to_data()

    assert crds_dict["UUID"] == "10000000-0000-0000-00000-000000000001"
    assert crds_dict["datasources"][0] == "10000000-0000-0000-00000-000000000001"
    assert crds_dict["metadata"]["site"] == "bsd"
    assert crds_dict["metadata"]["instrument"] == "picarro"


def test_from_data(crds):
    data = crds.to_data()
    new_crds = CRDS.from_data(data)

    start = datetime_to_datetime(datetime.datetime(2014, 1, 30, 10, 52, 30))
    end = datetime_to_datetime(datetime.datetime(2014, 1, 30, 14, 20, 30))

    assert new_crds._start_datetime == start
    assert new_crds._end_datetime == end

def test_save_and_load():
    filename = "bsd.picarro.1minute.248m.dat"
    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/CRDS"
    filepath = os.path.join(dir_path, test_data, filename)

    crds = CRDS.read_file(filepath=filepath)

    bucket = get_local_bucket(empty=True)
    crds.save(bucket=bucket)

    # Get slice of data
    one = crds._datasources[0]._data[0].head(1)
    two = crds._datasources[1]._data[0].head(1)
    three = crds._datasources[2]._data[0].head(1)

    uuid = crds._uuid
    loaded_crds = CRDS.load(uuid=uuid, bucket=bucket)

    start = datetime_to_datetime(datetime.datetime(2014, 1, 30, 10, 52, 30))
    end = datetime_to_datetime(datetime.datetime(2014, 1, 30, 14, 20, 30))

    loaded_one = loaded_crds._datasources[0]._data[0].head(1)
    loaded_two = loaded_crds._datasources[1]._data[0].head(1)
    loaded_three = loaded_crds._datasources[2]._data[0].head(1)

    assert loaded_crds._start_datetime == start
    assert loaded_crds._end_datetime == end
    assert loaded_one.equals(one)
    assert loaded_two.equals(two)
    assert loaded_three.equals(three)
    











