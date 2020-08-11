import datetime
import logging
import os
import uuid
from pathlib import Path

import pandas as pd
import pytest
from Acquire.ObjectStore import datetime_to_datetime, datetime_to_string

from HUGS.Modules import CRDS, Datasource
from HUGS.ObjectStore import get_local_bucket

mpl_logger = logging.getLogger("matplotlib")
mpl_logger.setLevel(logging.WARNING)

# @pytest.fixture(scope="session")
# def data():
#     filename = "bsd.picarro.1minute.248m.dat"
#     dir_path = os.path.dirname(__file__)
#     test_data = "../data/proc_test_data/CRDS"
#     filepath = os.path.join(dir_path, test_data, filename)


#     return pd.read_csv(filepath, header=None, skiprows=1, sep=r"\s+")


@pytest.fixture(autouse=True)
def hfd_filepath():
    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/CRDS"
    filename = "hfd.picarro.1minute.100m.min.dat"

    filepath = os.path.join(dir_path, test_data, filename)

    return filepath


@pytest.fixture(autouse=True)
def crds():
    get_local_bucket(empty=True)
    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/CRDS"
    filename = "hfd.picarro.1minute.100m.min.dat"

    filepath = os.path.join(dir_path, test_data, filename)
    CRDS.read_file(data_filepath=filepath, source_name="hfd_picarro_100m", site="hfd")
    crds = CRDS.load()

    return crds


def test_read_file(crds):
    uuids = crds.datasource_names()

    assert len(uuids) == 3

    ch4_datasouce = Datasource.load(uuid=uuids["hfd_picarro_100m_ch4"], shallow=False)
    co2_datasouce = Datasource.load(uuid=uuids["hfd_picarro_100m_co2"], shallow=False)
    co_datasouce = Datasource.load(uuid=uuids["hfd_picarro_100m_co"], shallow=False)

    date_key = "2013-12-04-14:02:30+00:00_2013-12-25-22:56:30+00:00"

    ch4_data = ch4_datasouce._data[date_key]
    co2_data = co2_datasouce._data[date_key]
    co_data = co_datasouce._data[date_key]

    assert ch4_data["ch4"][0].values == pytest.approx(1993.83)
    assert ch4_data["ch4_stdev"][0].values == pytest.approx(1.555)
    assert ch4_data["ch4_n_meas"][0].values == pytest.approx(19.0)

    assert co2_data["co2"][0] == pytest.approx(414.21)
    assert co2_data["co2_stdev"][0] == pytest.approx(0.109)
    assert co2_data["co2_n_meas"][0] == pytest.approx(19.0)

    assert co_data["co"][0] == pytest.approx(214.28)
    assert co_data["co_stdev"][0] == pytest.approx(4.081)
    assert co_data["co_n_meas"][0] == pytest.approx(19.0)


def test_read_data():

    crds = CRDS.load()

    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/CRDS"
    filename = "tac.picarro.1minute.100m.test.dat"

    filepath = os.path.join(dir_path, test_data, filename)

    filepath = Path(filepath)

    combined = crds.read_data(data_filepath=filepath, site="tac")

    assert len(combined) == 2

    assert list(combined.keys()) == ["ch4", "co2"]

    ch4_metadata = combined["ch4"]["metadata"]

    assert ch4_metadata["site"] == "tac"
    assert ch4_metadata["instrument"] == "picarro"
    assert ch4_metadata["time_resolution"] == "1_minute"
    assert ch4_metadata["inlet"] == "100m"
    assert ch4_metadata["port"] == "9"
    assert ch4_metadata["type"] == "air"
    assert ch4_metadata["species"] == "ch4"

    ch4_data = combined["ch4"]["data"]

    assert ch4_data.time[0] == pd.Timestamp("2012-07-31 14:50:30")
    assert ch4_data["ch4"][0] == pytest.approx(1905.28)
    assert ch4_data["ch4 stdev"][0] == pytest.approx(0.268)
    assert ch4_data["ch4 n_meas"][0] == pytest.approx(20)


def test_data_persistence(crds):
    first_store = crds.datasources()
    crds.save()
    crds = CRDS.load()

    second_store = crds.datasources()

    assert first_store == second_store


def test_seen_before_raises():
    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/CRDS"
    filename = "hfd.picarro.1minute.100m.min.dat"

    filepath = os.path.join(dir_path, test_data, filename)
    get_local_bucket(empty=True)

    CRDS.read_file(data_filepath=filepath, source_name="hfd_picarro_100m", site="hfd")

    crds = CRDS.load()
    crds.save()
    crds = CRDS.load()

    with pytest.raises(ValueError):
        CRDS.read_file(
            data_filepath=filepath, source_name="hfd_picarro_100m", site="hfd"
        )


def test_seen_before_overwrite():
    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/CRDS"
    filename = "hfd.picarro.1minute.100m.min.dat"

    filepath = os.path.join(dir_path, test_data, filename)
    get_local_bucket(empty=True)

    uuids_first = CRDS.read_file(
        data_filepath=filepath, source_name="hfd_picarro_100m", site="hfd"
    )

    crds = CRDS.load()
    crds.save()
    crds = CRDS.load()

    # Read the same file in again
    uuids_second = CRDS.read_file(
        data_filepath=filepath,
        source_name="hfd_picarro_100m",
        site="hfd",
        overwrite=True,
    )

    assert uuids_first == uuids_second


def test_to_data(crds):
    data = crds.to_data()

    datasource_names = [
        "hfd_picarro_100m_co",
        "hfd_picarro_100m_ch4",
        "hfd_picarro_100m_co2",
    ]

    assert data["stored"] is False
    assert sorted(data["datasource_names"]) == sorted(datasource_names)
    assert list(data["file_hashes"].values()) == ["hfd.picarro.1minute.100m.min.dat"]


def test_from_data(crds):
    data = crds.to_data()

    epoch = datetime_to_datetime(datetime.datetime(1970, 1, 1, 1, 1))

    data["creation_datetime"] = datetime_to_string(epoch)

    random_data1 = uuid.uuid4()
    random_data2 = uuid.uuid4()

    data["file_hashes"] = {"test1": random_data1, "test2": random_data2}

    c = CRDS.from_data(data)

    assert c._creation_datetime == epoch
    assert sorted(c._datasource_names) == sorted(
        ["hfd_picarro_100m_ch4", "hfd_picarro_100m_co2", "hfd_picarro_100m_co"]
    )

    assert c._file_hashes == {"test1": random_data1, "test2": random_data2}


def test_gas_info(crds, hfd_filepath):
    data = pd.read_csv(
        hfd_filepath,
        header=None,
        skiprows=1,
        sep=r"\s+",
        index_col=["0_1"],
        parse_dates=[[0, 1]],
    )

    n_gases, n_cols = crds._gas_info(data=data)

    assert n_gases == 3
    assert n_cols == 3


def test_clear_datasources(crds):
    assert len(crds.datasources()) == 3
    crds.clear_datasources()
    assert len(crds.datasources()) == 0


def test_add_datasources(crds):
    crds.clear_datasources()

    new_datasources = {
        "test1": "f619755d-6c6d-4182-9e42-1ddf1c7e4eb6",
        "test2": "04f3aa66-bafb-4bb5-9409-8bd5b5c64527",
        "test3": "eaff5063-a481-44d0-b8fa-1826666ad9db",
    }

    crds.add_datasources(new_datasources)

    assert sorted(crds.datasources()) == sorted(list(new_datasources.values()))
