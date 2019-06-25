import datetime
import os
import pytest
import uuid
import pandas as pd

from Acquire.ObjectStore import ObjectStore
from Acquire.ObjectStore import string_to_encoded

from HUGS.ObjectStore import get_dated_object
from HUGS.ObjectStore import get_dated_object_json
from HUGS.Modules import Datasource
from HUGS.Processing import Metadata
from HUGS.ObjectStore import get_local_bucket

mocked_uuid = "00000000-0000-0000-00000-000000000000"

mocked_uuid2 = "10000000-0000-0000-00000-000000000001"

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


@pytest.fixture
def mock_uuid2(monkeypatch):

    def mock_uuid():
        return mocked_uuid2

    monkeypatch.setattr(uuid, 'uuid4', mock_uuid)


# @pytest.fixture(scope="session")
# def save_data_in_store(mock_uuid):
#     bucket = local_bucket.get_local_bucket()
#     datasource = get_datasources(raw_data=data)[0]
#     original_slice = datasource._data.head(1)

#     # data = Datasource.create(name="test_name", instrument="test_instrument", site="test_site",
#     #                          network="test_network", data=datasource._data)
#     # data.save(bucket)

#     print("Save data in store uuid : ", datasource._uuid)

#     datasource.save(bucket)

def test_save(mock_uuid2):
    bucket = get_local_bucket()

    datasource = Datasource.create(name="test_name", instrument="test_instrument", site="test_site", network="test_network")

    datasource.save(bucket)

    prefix = "%s/uuid/%s" % (Datasource._datasource_root, datasource._uuid)

    objs = ObjectStore.get_all_object_names(bucket, prefix)

    assert objs[0].split("/")[-1] == mocked_uuid2


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

# def test_save_with_data(mock_uuid, data):
#     from processing._segment import get_datasources

#     bucket = local_bucket.get_local_bucket()

#     datasources = get_datasources(raw_data=data)
#     datasource  = datasources[0]
        
#     original_slice = datasource._data[0].head(1)

#     datasource.save(bucket)

#     old_uuid = datasource._uuid

#     new_datasource = Datasource.load(bucket, uuid=old_uuid)

#     new_slice = new_datasource._data[0].head(1)

#     assert new_slice.equals(original_slice)
    

# def test_save_multiple_with_data(data):
#     from processing._segment import get_datasources

#     bucket = local_bucket.get_local_bucket()

#     datasources = get_datasources(raw_data=data)
    
#     datasource_uuids = []
#     for d in datasources:
#         d.save(bucket=bucket)
#         datasource_uuids.append(d._uuid)
        
#     new_datasources = [Datasource.load(bucket=bucket, uuid=i) for i in datasource_uuids]

#     from objectstore._hugs_objstore import get_dated_object
#     dataframes = []
#     # for u in datasource_uuids:
#     #     key = "%s/uuid/%s" % (Datasource._data_root, u)
#     #     obj = get_dated_object(bucket=bucket, key=key)
#     #     dat = Datasource.dataframe_from_hdf(obj)


#     # old_uuid = datasource._uuid

#     # new_datasource = Datasource.load(bucket, uuid=old_uuid)

#     # new_slice = new_datasource._data.head(1)

#     assert False

def test_from_data(mock_uuid):
    
    datasource = Datasource.create(name="test_name_two", instrument="test_instrument_two",
                                              site="test_site_two", network="test_network_two")

    bucket = get_local_bucket()

    data = datasource.to_data()
    new_datasource = Datasource.from_data(bucket=bucket, data=datasource.to_data(), shallow=False)

    assert new_datasource._name == "test_name_two"
    assert new_datasource._uuid == mocked_uuid

    assert new_datasource._labels["instrument"] == "test_instrument_two"
    assert new_datasource._labels["site"] == "test_site_two"
    assert new_datasource._labels["network"] == "test_network_two"

def test_get_uid_from_name(mock_uuid2):
    from Acquire.ObjectStore import string_to_encoded

    bucket = get_local_bucket()

    name = "test_name"

    found_uuid = Datasource._get_uid_from_name(bucket, name)

    assert found_uuid == mocked_uuid2


def test_get_name_from_uid(mock_uuid):
    bucket = get_local_bucket()

    name = Datasource._get_name_from_uid(bucket, mocked_uuid2)

    assert name == "test_name"





# def test_load(mock_uuid, datasource):
#     bucket = local_bucket.get_local_bucket()

#     loaded_datasource = Datasource.load(bucket=bucket, uuid=mocked_uuid)

#     assert loaded_datasource._name == "test_name"
#     assert loaded_datasource._uuid == mocked_uuid
#     assert loaded_datasource._instrument == "test_instrument"
#     assert loaded_datasource._site == "test_site"
#     assert loaded_datasource._network == "test_network"









    
