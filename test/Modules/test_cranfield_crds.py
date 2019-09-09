import datetime
import os
import pytest
import uuid

from HUGS.Modules import Datasource, Cranfield_CRDS
from HUGS.ObjectStore import get_local_bucket, get_object_names

from Acquire.ObjectStore import string_to_datetime
from Acquire.ObjectStore import datetime_to_datetime


@pytest.fixture(autouse=True)
def before_tests():
    bucket = get_local_bucket(empty=True)
    crds = Cranfield_CRDS.create()
    crds.save()

def test_read_file():
    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/Cranfield_CRDS"
    filename = "thames_barrier_cumulative_calibrated_hourly_means_TEST.csv"

    filepath = os.path.join(dir_path, test_data, filename)
#    bucket = get_local_bucket(empty=True)

    crds = Cranfield_CRDS.read_file(data_filepath=filepath)

    # Get the data from the object store and ensure it's been read correctly
    datasources = [Datasource.load(uuid=uuid, shallow=False) for uuid in crds.datasources()]

    assert datasources[0].data()[0]["ch4"].iloc[10] == pytest.approx(2075.01711)
