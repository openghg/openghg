import logging
import os
from pathlib import Path
import pandas as pd
import pytest

from openghg.modules import Datasource, BTT
from openghg.objectstore import get_local_bucket

mpl_logger = logging.getLogger("matplotlib")
mpl_logger.setLevel(logging.WARNING)

# Disable this for long strings below - Line break occurred before a binary operator (W503)
# flake8: noqa: W503
def get_datapath(filename, data_type):
    return Path(__file__).resolve(strict=True).parent.joinpath(f"../data/proc_test_data/{data_type}/{filename}")


def test_read_file():
    btt = BTT()

    filepath = get_datapath(filename="BTT_test.csv", data_type="LGHG")

    data = btt.read_file(data_filepath=filepath)

    co2_data = data["CO2"]["data"]
    ch4_data = data["CH4"]["data"]

    assert co2_data.time[0] == pd.Timestamp("2019-01-14T09:30:00.00")
    assert co2_data["co2"][0] == pytest.approx(420.4700671)
    assert co2_data.time[-1] == pd.Timestamp("2019-01-14T14:00:00")
    assert co2_data["co2"][-1] == pytest.approx(413.45942912)

    assert ch4_data.time[0] == pd.Timestamp("2019-01-14T09:30:00.00")
    assert ch4_data["ch4"][0] == pytest.approx(1957.23980459)
    assert ch4_data.time[-1] == pd.Timestamp("2019-01-14T14:00:00")
    assert ch4_data["ch4"][-1] == pytest.approx(1961.72216725)

    del co2_data.attrs["File created"]
    del ch4_data.attrs["File created"]

    expected_attrs = {
        "data_owner": "Carole Helfter",
        "data_owner_email": "caro2@ceh.ac.uk",
        "inlet_height_magl": "192m",
        "instrument": "Picarro 2311-f",
        "Conditions of use": "Ensure that you contact the data owner at the outset of your project.",
        "Source": "In situ measurements of air",
        "Conventions": "CF-1.6",
        "Processed by": "OpenGHG_Cloud",
        "species": "co2",
        "Calibration_scale": "unknown",
        "station_longitude": -0.1389,
        "station_latitude": 51.5215,
        "station_long_name": "BT Tower, UK",
        "sampling_period": 1800
    }

    assert co2_data.attrs == expected_attrs

    expected_attrs["species"] = "ch4"
    
    assert ch4_data.attrs == expected_attrs
