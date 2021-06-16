import logging
import os
from pathlib import Path
import pandas as pd
import pytest

from openghg.modules import Datasource, NPL
from openghg.objectstore import get_local_bucket

mpl_logger = logging.getLogger("matplotlib")
mpl_logger.setLevel(logging.WARNING)

# Disable this for long strings below - Line break occurred before a binary operator (W503)
# flake8: noqa: W503
def get_datapath(filename, data_type):
    return Path(__file__).resolve(strict=True).parent.joinpath(f"../data/proc_test_data/{data_type}/{filename}")


def test_read_file():
    npl = NPL()

    filepath = get_datapath(filename="NPL_test.csv", data_type="LGHG")

    data = npl.read_file(data_filepath=filepath, sampling_period=60)

    co2_data = data["CO2"]["data"]
    ch4_data = data["CH4"]["data"]

    assert co2_data.time[0] == pd.Timestamp("2020-06-12")
    assert co2_data["co2"][0] == pytest.approx(424.1672774)
    assert co2_data.time[-1] == pd.Timestamp("2020-07-01T00:24:00")
    assert co2_data["co2"][-1] == pytest.approx(419.9544809)

    assert ch4_data.time[0] == pd.Timestamp("2020-06-12")
    assert ch4_data["ch4"][0] == pytest.approx(2004.462127)
    assert ch4_data.time[-1] == pd.Timestamp("2020-07-01T00:24:00")
    assert ch4_data["ch4"][-1] == pytest.approx(1910.546256)

    del co2_data.attrs["File created"]
    del ch4_data.attrs["File created"]

    expected_attrs = {
        "data_owner": "Tim Arnold",
        "data_owner_email": "tim.arnold@npl.co.uk",
        "Notes": "Rooftop instrument at NPL campus in Teddington",
        "inlet_height_magl": "17m",
        "instrument": "Picarro G2401",
        "Conditions of use": "Ensure that you contact the data owner at the outset of your project.",
        "Source": "In situ measurements of air",
        "Conventions": "CF-1.6",
        'Processed by': 'OpenGHG_Cloud',
        "species": "co2",
        "Calibration_scale": "unknown",
        "station_longitude": -0.3487,
        "station_latitude": 51.4241,
        "station_long_name": "National Physical Laboratory",
        "station_height_masl": 0,
    }

    assert co2_data.attrs == expected_attrs

    expected_attrs["species"] = "ch4"

    assert ch4_data.attrs == expected_attrs
