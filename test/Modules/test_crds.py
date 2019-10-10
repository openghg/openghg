import datetime
import os
import pytest
import uuid

from HUGS.Modules import Datasource, CRDS
from HUGS.ObjectStore import get_local_bucket, get_object_names

from Acquire.ObjectStore import string_to_datetime
from Acquire.ObjectStore import datetime_to_datetime

# @pytest.fixture(scope="session")
# def data():
#     filename = "bsd.picarro.1minute.248m.dat"
#     dir_path = os.path.dirname(__file__)
#     test_data = "../data/proc_test_data/CRDS"
#     filepath = os.path.join(dir_path, test_data, filename)

#     return pd.read_csv(filepath, header=None, skiprows=1, sep=r"\s+")

@pytest.fixture(autouse=True)
def before_tests():
    crds = CRDS.create()
    crds.save()

# def test_read_bsd():
#     dir_path = os.path.dirname(__file__)
#     test_data = "../data/search_data"
#     filename = "bsd.picarro.1minute.108m.min.dat"

#     filepath = os.path.join(dir_path, test_data, filename)
#     bucket = get_local_bucket(empty=True)

#     crds = CRDS.read_file(data_filepath=filepath)
#     # Get the data from the object store and ensure it's been read correctly
#     datasources = [Datasource.load(uuid=uuid, shallow=False) for uuid in crds.datasources()]

#     print(len(datasources))

#     assert False


def test_read_file():
    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/CRDS"
    filename = "hfd.picarro.1minute.100m_min.dat"

    filepath = os.path.join(dir_path, test_data, filename)
    bucket = get_local_bucket(empty=True)

    crds = CRDS.read_file(data_filepath=filepath, source_name="hfd_picarro_100m")

    # Get the data from the object store and ensure it's been read correctly
    datasources = [Datasource.load(uuid=uuid, shallow=False) for uuid in crds.datasources()]

    data_one = datasources[0].data()
    assert data_one[0]["ch4 count"].iloc[0] == pytest.approx(1993.83)
    assert data_one[0]["ch4 stdev"].iloc[0] == pytest.approx(1.555)
    assert data_one[0]["ch4 n_meas"].iloc[0] == pytest.approx(19.0)

    data_two = datasources[1].data()

    assert data_two[0]["co2 count"].iloc[0] == pytest.approx(414.21)
    assert data_two[0]["co2 stdev"].iloc[0] == pytest.approx(0.109)
    assert data_two[0]["co2 n_meas"].iloc[0] == pytest.approx(19.0)

    data_three = datasources[2].data()

    assert data_three[0]["co count"].iloc[0] == pytest.approx(214.28)
    assert data_three[0]["co stdev"].iloc[0] == pytest.approx(4.081)
    assert data_three[0]["co n_meas"].iloc[0] == pytest.approx(19.0)


def test_data_persistence():
    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/CRDS"
    filename = "hfd.picarro.1minute.100m_min.dat"

    filepath = os.path.join(dir_path, test_data, filename)
    bucket = get_local_bucket(empty=True)

    crds = CRDS.read_file(data_filepath=filepath, source_name="hfd_picarro_100m")

    first_store = crds.datasources()

    crds.save()

    crds = CRDS.load()

    CRDS.read_file(data_filepath=filepath, source_name="hfd_picarro_100m")

    second_store = crds.datasources()

    print(first_store, second_store)
    
    assert first_store == second_store



    # datasources = [Datasource.load(uuid=uuid, shallow=False) for uuid in crds.datasources()]

    # for d in datasources:
    #     print(d.uuid())


# def test_read_folder():
#     folder_path = "/home/gar/Documents/Devel/hugs/raw_data/CRDS_picarro"
#     CRDS.read_folder(folder_path=folder_path)
#     bucket = get_local_bucket()
#     objs = get_object_names(bucket=bucket)

#     # print(objs)

#     assert False

# @pytest.fixture
# def mock_uuid(monkeypatch):
#     def mock_uuid():
#         return mocked_uuid

#     monkeypatch.setattr(uuid, 'uuid4', mock_uuid)

# @pytest.fixture
# def crds(mock_uuid):
#     filename = "bsd.picarro.1minute.248m.dat"
#     dir_path = os.path.dirname(__file__)
#     test_data = "../data/proc_test_data/CRDS"
#     filepath = os.path.join(dir_path, test_data, filename)

#     return CRDS.read_file(data_filepath=filepath)

# def test_read_file(crds):
#     start = datetime.datetime(2014,1,30,10,52,30)
#     end = datetime.datetime(2014, 1, 30, 14, 20, 30)

#     assert crds._start_datetime == start
#     assert crds._end_datetime == end
#     assert crds._uuid == mocked_uuid


# def test_to_data(crds):
#     crds_dict = crds.to_data()

#     assert crds_dict["UUID"] == "10000000-0000-0000-00000-000000000001"
#     assert crds_dict["datasources"][0] == "10000000-0000-0000-00000-000000000001"
#     assert crds_dict["metadata"]["site"] == "bsd"
#     assert crds_dict["metadata"]["instrument"] == "picarro"


# def test_from_data(crds):
#     data = crds.to_data()
#     new_crds = CRDS.from_data(data)

#     start = datetime_to_datetime(datetime.datetime(2014, 1, 30, 10, 52, 30))
#     end = datetime_to_datetime(datetime.datetime(2014, 1, 30, 14, 20, 30))

#     assert new_crds._start_datetime == start
#     assert new_crds._end_datetime == end

# def test_save_and_load():
#     filename = "bsd.picarro.1minute.248m.dat"
#     dir_path = os.path.dirname(__file__)
#     test_data = "../data/proc_test_data/CRDS"
#     filepath = os.path.join(dir_path, test_data, filename)

#     crds = CRDS.read_file(data_filepath=filepath)

#     bucket = get_local_bucket(empty=True)
#     crds.save(bucket=bucket)

#     # Get slice of data
#     one = crds._datasources[0]._data[0].head(1)
#     two = crds._datasources[1]._data[0].head(1)
#     three = crds._datasources[2]._data[0].head(1)

#     uuid = crds._uuid
#     loaded_crds = CRDS.load(uuid=uuid, bucket=bucket)

#     start = datetime_to_datetime(datetime.datetime(2014, 1, 30, 10, 52, 30))
#     end = datetime_to_datetime(datetime.datetime(2014, 1, 30, 14, 20, 30))

#     loaded_one = loaded_crds._datasources[0]._data[0].head(1)
#     loaded_two = loaded_crds._datasources[1]._data[0].head(1)
#     loaded_three = loaded_crds._datasources[2]._data[0].head(1)

#     assert loaded_crds._start_datetime == start
#     assert loaded_crds._end_datetime == end
#     assert loaded_one.equals(one)
#     assert loaded_two.equals(two)
#     assert loaded_three.equals(three)
    











