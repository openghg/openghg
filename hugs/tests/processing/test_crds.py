import datetime
import os
import pandas as pd
import pytest
import uuid

from Acquire.ObjectStore import ObjectStore as _ObjectStore
from objectstore.local_bucket import get_local_bucket
from objectstore.hugs_objstore import get_object_names

from processing._crds import CRDS

mocked_uuid = "00000000-0000-0000-00000-000000000000"

@pytest.fixture
def mock_uuid(monkeypatch):
    def mock_uuid():
        return mocked_uuid
    monkeypatch.setattr(uuid, 'uuid4', mock_uuid)

@pytest.fixture(scope="session")
def crds():
    filename = "bsd.picarro.1minute.248m.dat"
    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/CRDS"
    filepath = os.path.join(dir_path, test_data, filename)

    return CRDS.read_file(filepath)


def test_create():
    filename = "bsd.picarro.1minute.248m.dat"
    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/CRDS"
    filepath = os.path.join(dir_path, test_data, filename)

    crds = CRDS.read_file(filepath)

    first_datetime = crds._datasources[0]._data[0].first_valid_index()

    # TODO - check timestamp str and conversion to datetime    
    assert first_datetime == pd.Timestamp("2014-01-30 10:52:30")

def test_save_and_load(crds):
    filename = "bsd.picarro.1minute.248m.dat"
    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/CRDS"
    filepath = os.path.join(dir_path, test_data, filename)

    crds = CRDS.read_file(filepath)

    uuid_to_load = crds._uuid

    bucket = get_local_bucket()

    crds.save(bucket)

    new_crds = CRDS.load(bucket=bucket, uuid=uuid_to_load)

    # print(new_crds._datasources[0])

    gas_names = [d._name for d in new_crds._datasources]

    valid_gases = ["ch4", "co", "co2"]

    objects = _ObjectStore.get_all_object_names(bucket=bucket)

    assert sorted(gas_names) == sorted(valid_gases)


def test_search_store(crds):
    bucket = get_local_bucket()
    # Create and store data
    crds.save(bucket=bucket)

    start = datetime.datetime.strptime("2014-01-30", "%Y-%m-%d")
    end = datetime.datetime.strptime("2014-01-31","%Y-%m-%d")

    keys = crds.search_store(bucket=bucket, root_path="datasource", start_datetime=start, end_datetime=end)
    
    # TODO - better test for this
    assert len(keys) == 3


def test_search_store_two():
    # Load in the new data
    filename = "hfd.picarro.1minute.100m_min.dat"
    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/CRDS"
    filepath = os.path.join(dir_path, test_data, filename)

    crds = CRDS.read_file(filepath)

    bucket = get_local_bucket(empty=True)
    # Create and store data
    crds.save(bucket=bucket)

    start = datetime.datetime.strptime("2013-01-01", "%Y-%m-%d")
    end = datetime.datetime.strptime("2019-06-01", "%Y-%m-%d")

    keys = crds.search_store(bucket=bucket, root_path="datasource", start_datetime=start, end_datetime=end)

    # TODO - better test for this
    # 21 as 7 years * 3 gases
    assert len(keys) == 21

