import logging
import os

from pathlib import Path
from pandas import Timestamp
import pytest

from HUGS.Modules import NOAA, Datasource
from HUGS.ObjectStore import get_local_bucket

mpl_logger = logging.getLogger("matplotlib")
mpl_logger.setLevel(logging.WARNING)

# Disable this for long strings below - Line break occurred before a binary operator (W503)
# flake8: noqa: W503


def get_datapath(filename, data_type):
    return (
        Path(__file__)
        .resolve(strict=True)
        .parent.joinpath(f"../data/proc_test_data/{data_type}/{filename}")
    )


def test_read_file():
    noaa = NOAA()

    filepath = get_datapath(
        filename="co_pocn25_surface-flask_1_ccgg_event.txt", data_type="NOAA"
    )

    data = noaa.read_file(data_filepath=filepath, species="CO")

    assert data["co"]["metadata"] == {
        "species": "co",
        "site": "POC",
        "measurement_type": "flask",
        "network": "NOAA"
    }

    co_data = data["co"]["data"]

    assert co_data.time[0] == Timestamp("1990-06-29T05:00:00")
    assert co_data["co"][0] == pytest.approx(94.9)
    assert co_data["co_repeatability"][0] == pytest.approx(-999.99)
    assert co_data["co_selection_flag"][0] == 0

    assert co_data.time[-1] == Timestamp("2017-07-15T04:15:00")
    assert co_data["co"][-1] == pytest.approx(73.16)
    assert co_data["co_repeatability"][-1] == pytest.approx(-999.99)
    assert co_data["co_selection_flag"][-1] == 0

    attrs = co_data.attrs

    expected_attrs = {
        "data_owner": "Ed Dlugokencky, Gabrielle Petron (CO)",
        "data_owner_email": "ed.dlugokencky@noaa.gov, gabrielle.petron@noaa.gov",
        "inlet_height_magl": "NA",
        "instrument": "GC-HgO-VUV",
        "Conditions of use": "Ensure that you contact the data owner at the outset of your project.",
        "Source": "In situ measurements of air",
        "Conventions": "CF-1.6",
        "Processed by": "auto@hugs-cloud.com",
        "species": "co",
        "Calibration_scale": "unknown",
        "station_longitude": -175,
        "station_latitude": 28,
        "station_long_name": "Pacific Ocean",
        "station_height_masl": 0.0,
    }

    del attrs["File created"]

    assert attrs == expected_attrs


def test_read_data():
    noaa = NOAA()

    filepath = get_datapath(filename="co_pocn25_surface-flask_1_ccgg_event.txt", data_type="NOAA")

    data = noaa.read_data(data_filepath=filepath, species="CO")

    co_data = data["co"]["data"]
    metadata = data["co"]["metadata"]
    attributes = data["co"]["attributes"]

    expected_metadata = {"species": "co", "site": "POC", "measurement_type": "flask", "network": "NOAA"}

    expected_attrs = {'data_owner': 'Ed Dlugokencky, Gabrielle Petron (CO)', 
                    'data_owner_email': 'ed.dlugokencky@noaa.gov, gabrielle.petron@noaa.gov', 
                    'inlet_height_magl': 'NA', 'instrument': 'GC-HgO-VUV'}
    

    assert co_data["CO"][0] == 94.9
    assert co_data["CO"][-1] == 73.16
    assert co_data.time[0] == Timestamp("1990-06-29T05:00:00.000000000")
    assert co_data.time[-1] == Timestamp("2017-07-15T04:15:00.000000000")

    assert metadata == expected_metadata
    assert attributes == expected_attrs
