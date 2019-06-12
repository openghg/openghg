import datetime
import os
import pytest

from modules import CRDS
from objectstore import get_local_bucket
from processing import in_daterange
from processing import search_store
from processing import key_to_daterange
from processing import gas_search

from modules import Instrument

from Acquire.ObjectStore import datetime_to_string
from Acquire.ObjectStore import datetime_to_datetime

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

    # Get the instrument
    instrument_uuids = list(crds._instruments)

    # Get UUID from Instrument
    instrument = Instrument.load(bucket=bucket, uuid=instrument_uuids[0])
    # Get Datasource IDs from Instrument
    data_uuids = [d._uuid for d in instrument._datasources]

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

    bucket = get_local_bucket()
    # Create and store data
    crds.save(bucket=bucket)

    # Get the instrument
    instrument_uuids = list(crds._instruments)

    # Get UUID from Instrument
    instrument = Instrument.load(bucket=bucket, uuid=instrument_uuids[0])
    # Get Datasource IDs from Instrument
    data_uuids = [d._uuid for d in instrument._datasources]

    start = datetime.datetime.strptime("2013-01-01", "%Y-%m-%d")
    end = datetime.datetime.strptime("2019-06-01", "%Y-%m-%d")
    

    keys = search_store(bucket=bucket, data_uuids=data_uuids, root_path="datasource",
                        start_datetime=start, end_datetime=end)

    # TODO - better test for this
    # 21 as 7 years * 3 gases
    assert len(keys) == 21


def test_in_daterange():
    start = datetime_to_datetime(datetime.datetime.strptime("2013-01-01", "%Y-%m-%d"))
    end = datetime_to_datetime(datetime.datetime.strptime("2019-06-01", "%Y-%m-%d"))
    start_key = datetime_to_datetime(datetime.datetime.strptime("2014-01-01", "%Y-%m-%d"))
    end_key = datetime_to_datetime(datetime.datetime.strptime("2018-06-01", "%Y-%m-%d"))

    daterange_str = "".join([datetime_to_string(start_key), "_", datetime_to_string(end_key)])

    key = "datasource/uuid/10000000-0000-0000-00000-000000000001/%s" % daterange_str

    assert in_daterange(key, start_search=start, end_search=end) == True
        

def test_key_to_daterange():
    start_key = datetime_to_datetime(datetime.datetime.strptime("2014-01-01", "%Y-%m-%d"))
    end_key = datetime_to_datetime(datetime.datetime.strptime("2018-06-01", "%Y-%m-%d"))

    daterange_str = "".join([datetime_to_string(start_key), "_", datetime_to_string(end_key)])

    key = "datasource/uuid/10000000-0000-0000-00000-000000000001/%s" % daterange_str

    start_back, end_back = key_to_daterange(key)

    assert start_back == start_key
    assert end_back == end_key


def test_gas_search(crds):
    # Create CRDS object and read it all in
    bucket = get_local_bucket()
    # Create and store data
    crds.save(bucket=bucket)

    gas_name = "co"
    meas_type = "CRDS"

    keys = gas_search(gas_name=gas_name, meas_type=meas_type)

    print(keys)

    assert False
