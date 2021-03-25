import logging
import os
from pathlib import Path
import pandas as pd
import pytest

from openghg.modules import Datasource, THAMESBARRIER
from openghg.objectstore import get_local_bucket

mpl_logger = logging.getLogger("matplotlib")
mpl_logger.setLevel(logging.WARNING)

# Disable this for long strings below - Line break occurred before a binary operator (W503)
# flake8: noqa: W503


def get_datapath(filename, data_type):
    return Path(__file__).resolve(strict=True).parent.joinpath(f"../data/proc_test_data/{data_type}/{filename}")


def test_read_file():
    tb = THAMESBARRIER()

    filepath = get_datapath(filename="thames_test_20190707.csv", data_type="THAMESBARRIER")

    data = tb.read_file(data_filepath=filepath)

    ch4_data = data["CH4"]["data"]
    co2_data = data["CO2"]["data"]
    co_data = data["CO"]["data"]

    assert ch4_data.time[0] == pd.Timestamp("2019-07-01T00:39:55.000000000")
    assert ch4_data["ch4"][0] == pytest.approx(1960.835716)
    assert ch4_data["ch4_variability"][0] == 0

    assert co2_data.time[0] == pd.Timestamp("2019-07-01T00:39:55.000000000")
    assert co2_data["co2"][0] == pytest.approx(417.973447)
    assert co2_data["co2_variability"][0] == 0

    assert co_data.time[0] == pd.Timestamp("2019-07-01T00:39:55.000000000")
    assert co_data["co"][0] == pytest.approx(0.08788712)
    assert co_data["co_variability"][0] == 0

    expected_attrs = {
        "data_owner": "Valerio Ferracci",
        "data_owner_email": "V.Ferracci@cranfield.ac.uk",
        "Notes": "~5m above high tide water level, in tidal region of the Thames",
        "inlet_height_magl": "5m",
        "instrument": "picarrog2401",
        "Conditions of use": "Ensure that you contact the data owner at the outset of your project.",
        "Source": "In situ measurements of air",
        "Conventions": "CF-1.6",
        "Processed by": "OpenGHG_Cloud",
        "species": "co",
        "Calibration_scale": "unknown",
        "station_longitude": 0.037,
        "station_latitude": 51.497,
        "station_long_name": "Thames Barrier, UK",
        "station_height_masl": 5.0,
    }

    del co_data.attrs["File created"]

    assert co_data.attrs == expected_attrs
