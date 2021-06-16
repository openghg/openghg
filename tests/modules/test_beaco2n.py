from openghg.modules import BEACO2N
from pandas import Timestamp
from pathlib import Path
import pytest


def get_datapath(filename, data_type):
    return Path(__file__).resolve(strict=True).parent.parent.joinpath(f"data/proc_test_data/{data_type}/{filename}")


def test_read_file():
    beacon = BEACO2N()
    filepath = get_datapath(filename="Charlton_Community_Center.csv", data_type="BEACO2N")

    result = beacon.read_file(data_filepath=filepath, sampling_period=1200)

    pm_data = result["pm"]["data"]
    co2_data = result["co2"]["data"]

    pm_metadata = result["pm"]["metadata"]
    co2_metadata = result["co2"]["metadata"]

    pm_metadata = result["pm"]["metadata"]
    co2_metadata = result["co2"]["metadata"]

    pm_attrs = result["pm"]["attributes"]
    co2_attrs = result["co2"]["attributes"]
    pm_data.time[0] == Timestamp("2015-04-18T04:00:00")
    pm_data["pm"][0] == 20.3
    pm_data["pm_qc"][0] == 2.0
    co2_data.time[0] == Timestamp("2015-04-18T04:00:00")
    co2_data["co2"][0] == 410.4
    co2_data["co2_qc"][0] == 2

    assert pm_metadata == {
        "units": "ug/m3",
        "site": "CHARLTONCOMMUNITYCENTER",
        "species": "pm",
        "inlet": "NA",
        "network": "beaco2n",
        "sampling_period": "1200",
    }
    assert co2_metadata == {
        "units": "ppm",
        "site": "CHARLTONCOMMUNITYCENTER",
        "species": "co2",
        "inlet": "NA",
        "network": "beaco2n",
        "sampling_period": "1200",
    }

    assert pm_attrs == {
        "deployed": "2017-01-24",
        "id": 75,
        "latitude": 29.688,
        "longitude": -95.276,
        "magl": 6.477,
        "masl": 9.9,
        "node_folder_id": 886,
        "comment": "Retrieved from http://beacon.berkeley.edu/",
    }

    assert co2_attrs == {
        "deployed": "2017-01-24",
        "id": 75,
        "latitude": 29.688,
        "longitude": -95.276,
        "magl": 6.477,
        "masl": 9.9,
        "node_folder_id": 886,
        "comment": "Retrieved from http://beacon.berkeley.edu/",
    }


def test_incorrect_file_read_raises():
    filepath = get_datapath(filename="incorrect_format.csv", data_type="BEACO2N")

    beacon = BEACO2N()
    with pytest.raises(ValueError):
        beacon.read_file(data_filepath=filepath)


def test_incorrect_site_raises():
    filepath = get_datapath(filename="Unknown_site.csv", data_type="BEACO2N")

    beacon = BEACO2N()

    with pytest.raises(ValueError):
        beacon.read_file(data_filepath=filepath)
