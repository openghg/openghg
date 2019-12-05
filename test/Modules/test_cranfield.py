import datetime
import os
import pytest
import uuid

from HUGS.Modules import Datasource, Cranfield
from HUGS.ObjectStore import get_local_bucket, get_object_names

from Acquire.ObjectStore import string_to_datetime
from Acquire.ObjectStore import datetime_to_datetime


@pytest.fixture(autouse=True)
def before_tests():
    bucket = get_local_bucket(empty=True)
    cranfield = Cranfield.create()
    cranfield.save()

def test_read_file():
    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/Cranfield_CRDS"
    filename = "thames_barrier_cumulative_calibrated_hourly_means_TEST.csv"

    filepath = os.path.join(dir_path, test_data, filename)

    uuids = Cranfield.read_file(data_filepath=filepath)

    # Get the data from the object store and ensure it's been read correctly
    # Here we sort by the names of the gases so hopefully this won't break
    datasources = [Datasource.load(uuid=uuid, shallow=False) for _, uuid in sorted(uuids.items())]
    
    data = [d.data() for d in datasources]

    ch4_data = data[0][0][0]
    co_data = data[1][0][0]
    co2_data = data[2][0][0]

    assert len(uuids) == 3

    # print(co_data.head())
    assert ch4_data["ch4"][0] == pytest.approx(2585.6510)
    assert ch4_data["ch4 variability"][0] == pytest.approx(75.502187065)

    assert co_data["co"][0] == pytest.approx(289.697545)
    assert co_data["co variability"][0] == pytest.approx(6.999084)

    assert co2_data["co2"][0] == pytest.approx(460.573223)
    assert co2_data["co2 variability"][0] == pytest.approx(0.226956417)

def test_read_data():
    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/Cranfield_CRDS"
    filename = "thames_barrier_cumulative_calibrated_hourly_means_TEST.csv"

    filepath = os.path.join(dir_path, test_data, filename)

    cranfield = Cranfield.load()

    combined_data = cranfield.read_data(data_filepath=filepath)

    print(sorted(combined_data.keys()))
    
    assert len(combined_data) == 3
    # assert sorted(combined_data.keys()) == 

    assert False
