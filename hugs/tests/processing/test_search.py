import datetime
import os
import pytest

from objectstore import get_local_bucket
from modules import CRDS
from processing import search_store

@pytest.fixture(scope="session")
def crds():
    filename = "bsd.picarro.1minute.248m.dat"
    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/CRDS"
    filepath = os.path.join(dir_path, test_data, filename)

    return CRDS.read_file(filepath)


def test_search_store(crds):
    bucket = get_local_bucket()
    # Create and store data
    crds.save(bucket=bucket)

    data_uuids = [d._uuid for d in crds._datasources]

    start = datetime.datetime.strptime("2014-01-30", "%Y-%m-%d")
    end = datetime.datetime.strptime("2014-01-31", "%Y-%m-%d")

    keys = search_store(bucket=bucket, data_uuids=data_uuids, root_path="datasource",
                        start_datetime=start, end_datetime=end)

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

    data_uuids = [d._uuid for d in crds._datasources]

    start = datetime.datetime.strptime("2013-01-01", "%Y-%m-%d")
    end = datetime.datetime.strptime("2019-06-01", "%Y-%m-%d")

    keys = search_store(bucket=bucket, data_uuids=data_uuids, root_path="datasource",
                        start_datetime=start, end_datetime=end)

    # TODO - better test for this
    # 21 as 7 years * 3 gases
    assert len(keys) == 21
