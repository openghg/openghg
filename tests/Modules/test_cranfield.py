import datetime
import os
import uuid

import pytest
from Acquire.ObjectStore import datetime_to_datetime, datetime_to_string

from HUGS.Modules import Cranfield, Datasource
from HUGS.ObjectStore import get_local_bucket


@pytest.fixture(autouse=True)
def cranfield_obj():
    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/Cranfield_CRDS"
    filename = "thames_barrier_cumulative_calibrated_hourly_means_TEST.csv"

    filepath = os.path.join(dir_path, test_data, filename)

    # Empty the local bucket
    get_local_bucket(empty=True)

    # Create a cranfield object as we won't have one with an empty bucket
    cranfield = Cranfield.create()
    cranfield.save()

    # Read in the data file and create datasources
    Cranfield.read_file(data_filepath=filepath)

    # We want the updated object from the object store
    cranfield = Cranfield.load()

    return cranfield


def test_read_file(cranfield_obj):
    # Get the data from the object store and ensure it's been read correctly
    # Here we sort by the names of the gases so hopefully this won't break
    uuids = cranfield_obj.datasources()

    ch4_datasouce = Datasource.load(
        uuid=uuids["thames_barrier_cumulative_calibrated_hourly_means_TEST_ch4"],
        shallow=False,
    )
    co2_datasouce = Datasource.load(
        uuid=uuids["thames_barrier_cumulative_calibrated_hourly_means_TEST_co2"],
        shallow=False,
    )
    co_datasouce = Datasource.load(
        uuid=uuids["thames_barrier_cumulative_calibrated_hourly_means_TEST_co"],
        shallow=False,
    )

    date_key = "2018-05-05-00:00:00+00:00_2018-05-13-16:00:00+00:00"

    ch4_data = ch4_datasouce._data[date_key]
    co2_data = co2_datasouce._data[date_key]
    co_data = co_datasouce._data[date_key]

    assert len(uuids) == 3

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

    assert len(combined_data) == 3

    species = ["co", "ch4", "co2"]
    assert sorted(species) == sorted(combined_data.keys())

    assert combined_data["co"]["metadata"] == {
        "site": "THB",
        "instrument": "CRDS",
        "time_resolution": "1_hour",
        "height": "10magl",
        "species": "co",
    }

    assert combined_data["co2"]["metadata"] == {
        "site": "THB",
        "instrument": "CRDS",
        "time_resolution": "1_hour",
        "height": "10magl",
        "species": "co2",
    }

    assert combined_data["ch4"]["metadata"] == {
        "site": "THB",
        "instrument": "CRDS",
        "time_resolution": "1_hour",
        "height": "10magl",
        "species": "ch4",
    }


def test_to_data(cranfield_obj):
    data = cranfield_obj.to_data()

    assert data["stored"] is False
    assert len(data["datasource_uuids"]) == 3
    assert sorted(data["datasource_names"].keys()) == sorted(
        [
            "thames_barrier_cumulative_calibrated_hourly_means_TEST_ch4",
            "thames_barrier_cumulative_calibrated_hourly_means_TEST_co2",
            "thames_barrier_cumulative_calibrated_hourly_means_TEST_co",
        ]
    )
    assert len(data["file_hashes"]) == 0


def test_from_data(cranfield_obj):
    data = cranfield_obj.to_data()

    epoch = datetime_to_datetime(datetime.datetime(1970, 1, 1, 1, 1))

    data["creation_datetime"] = datetime_to_string(epoch)

    random_data1 = uuid.uuid4()
    random_data2 = uuid.uuid4()

    data["file_hashes"] = {"test1": random_data1, "test2": random_data2}

    c = Cranfield.from_data(data)

    assert c._creation_datetime == epoch
    assert sorted(c._datasource_names) == sorted(
        [
            "thames_barrier_cumulative_calibrated_hourly_means_TEST_ch4",
            "thames_barrier_cumulative_calibrated_hourly_means_TEST_co2",
            "thames_barrier_cumulative_calibrated_hourly_means_TEST_co",
        ]
    )

    assert c._file_hashes == {"test1": random_data1, "test2": random_data2}


def test_exists(cranfield_obj):
    cranfield_obj.save()

    assert Cranfield.exists() is True


def test_clear_datasources(cranfield_obj):
    assert len(cranfield_obj.datasources()) == 3
    cranfield_obj.clear_datasources()
    assert len(cranfield_obj.datasources()) == 0


def test_add_datasources(cranfield_obj):
    cranfield_obj.clear_datasources()

    new_datasources = {
        "test1": "f619755d-6c6d-4182-9e42-1ddf1c7e4eb6",
        "test2": "04f3aa66-bafb-4bb5-9409-8bd5b5c64527",
        "test3": "eaff5063-a481-44d0-b8fa-1826666ad9db",
    }

    cranfield_obj.add_datasources(new_datasources)

    assert cranfield_obj.datasources() == new_datasources
