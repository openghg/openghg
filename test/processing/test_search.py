import datetime
import os
import pytest

from HUGS.Modules import CRDS
from HUGS.Modules import Instrument
from HUGS.ObjectStore import get_local_bucket
from HUGS.Processing import in_daterange, search_store, key_to_daterange, gas_search, load_object
                            

from Acquire.ObjectStore import datetime_to_string
from Acquire.ObjectStore import datetime_to_datetime

@pytest.fixture(scope="session")
def crds():
    filename = "bsd.picarro.1minute.248m.dat"
    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/CRDS"
    filepath = os.path.join(dir_path, test_data, filename)

    return CRDS.read_file(filepath)


def test_load_object(crds):
    bucket = get_local_bucket()
    crds.save(bucket)
    uuid = crds.uuid()
    class_name = "crds"
    obj = load_object(class_name=class_name, uuid=uuid)

    assert isinstance(obj, CRDS)
    assert obj.uuid() == crds.uuid()


def test_gas_search():
    filename = "bsd.picarro.1minute.248m.dat"
    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/CRDS"
    filepath = os.path.join(dir_path, test_data, filename)  

    _ = get_local_bucket(empty=True)

    crds = CRDS.read_file(filepath)

    gas_name = "co"
    data_type = "CRDS"

    keys = gas_search(species=gas_name, data_type=data_type)

    assert len(keys) == 7


# def test_search_store(crds):
#     bucket = get_local_bucket()
#     # Create and store data
#     crds.save(bucket=bucket)

#     # Get the instrument
#     instrument_uuids = list(crds._instruments)

#     # Get UUID from Instrument
#     instrument = Instrument.load(bucket=bucket, uuid=instrument_uuids[0])
#     # Get Datasource IDs from Instrument
#     data_uuids = [d._uuid for d in instrument._datasources]

#     start = datetime.datetime.strptime("2014-01-30", "%Y-%m-%d")
#     end = datetime.datetime.strptime("2014-01-31", "%Y-%m-%d")

#     keys = search_store(bucket=bucket, data_uuids=data_uuids, root_path="datasource",
#                         start_datetime=start, end_datetime=end)

#     # TODO - better test for this
#     assert len(keys) == 3


# def test_search_store_two():
#     # Load in the new data
#     filename = "hfd.picarro.1minute.100m_min.dat"
#     dir_path = os.path.dirname(__file__)
#     test_data = "../data/proc_test_data/CRDS"
#     filepath = os.path.join(dir_path, test_data, filename)

#     crds = CRDS.read_file(filepath)

#     bucket = get_local_bucket()
#     # Create and store data
#     crds.save(bucket=bucket)

#     # Get the instrument
#     instrument_uuids = list(crds._instruments)

#     # Get UUID from Instrument
#     instrument = Instrument.load(bucket=bucket, uuid=instrument_uuids[0])
#     # Get Datasource IDs from Instrument
#     data_uuids = [d._uuid for d in instrument._datasources]

#     start = datetime.datetime.strptime("2013-01-01", "%Y-%m-%d")
#     end = datetime.datetime.strptime("2019-06-01", "%Y-%m-%d")
    

#     keys = search_store(bucket=bucket, data_uuids=data_uuids, root_path="datasource",
#                         start_datetime=start, end_datetime=end)

#     # TODO - better test for this
#     # 21 as 7 years * 3 gases
#     assert len(keys) == 21


# def test_in_daterange():
#     start = datetime_to_datetime(datetime.datetime.strptime("2013-01-01", "%Y-%m-%d"))
#     end = datetime_to_datetime(datetime.datetime.strptime("2019-06-01", "%Y-%m-%d"))
#     start_key = datetime_to_datetime(datetime.datetime.strptime("2014-01-01", "%Y-%m-%d"))
#     end_key = datetime_to_datetime(datetime.datetime.strptime("2018-06-01", "%Y-%m-%d"))

#     daterange_str = "".join([datetime_to_string(start_key), "_", datetime_to_string(end_key)])

#     key = "datasource/uuid/10000000-0000-0000-00000-000000000001/%s" % daterange_str

#     assert in_daterange(key, start_search=start, end_search=end) == True
        

# def test_key_to_daterange():
#     start_key = datetime_to_datetime(datetime.datetime.strptime("2014-01-01", "%Y-%m-%d"))
#     end_key = datetime_to_datetime(datetime.datetime.strptime("2018-06-01", "%Y-%m-%d"))

#     daterange_str = "".join([datetime_to_string(start_key), "_", datetime_to_string(end_key)])

#     key = "datasource/uuid/10000000-0000-0000-00000-000000000001/%s" % daterange_str

#     start_back, end_back = key_to_daterange(key)

#     assert start_back == start_key
#     assert end_back == end_key

