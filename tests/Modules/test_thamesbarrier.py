import logging
import os

import pandas as pd
import pytest

from HUGS.Modules import Datasource, ThamesBarrier
from HUGS.ObjectStore import get_local_bucket

mpl_logger = logging.getLogger("matplotlib")
mpl_logger.setLevel(logging.WARNING)

# Disable this for long strings below - Line break occurred before a binary operator (W503)
# flake8: noqa: W503


def test_site_attributes():
    tb = ThamesBarrier()

    site_attributes = tb.site_attributes()

    assert (
        site_attributes["Notes"]
        == "~5m above high tide water level, in tidal region of the Thames"
    )
    assert site_attributes["inlet_height_magl"] == "5 m"
    assert site_attributes["instrument"] == "Picarro G2401"


def test_read_file():
    _ = get_local_bucket(empty=True)

    tb = ThamesBarrier()

    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/ThamesBarrier"
    filename = "thames_test_20190707.csv"

    filepath = os.path.join(dir_path, test_data, filename)

    tb.read_file(data_filepath=filepath, source_name="TMB")

    date_key = "2019-07-01-00:39:55+00:00_2019-08-01-00:10:30+00:00"

    ch4_ds = Datasource.load(uuid=uuids["TMB_CH4"])
    co2_ds = Datasource.load(uuid=uuids["TMB_CO2"])
    co_ds = Datasource.load(uuid=uuids["TMB_CO"])

    ch4_data = ch4_ds._data[date_key]
    co2_data = co2_ds._data[date_key]
    co_data = co_ds._data[date_key]

    assert ch4_data.time[0] == pd.Timestamp("2019-07-01T00:39:55.000000000")
    assert ch4_data["ch4"][0] == pytest.approx(1960.835716)
    assert ch4_data["ch4_variability"][0] == 0

    assert co2_data.time[0] == pd.Timestamp("2019-07-01T00:39:55.000000000")
    assert co2_data["co2"][0] == pytest.approx(417.973447)
    assert co2_data["co2_variability"][0] == 0

    assert co_data.time[0] == pd.Timestamp("2019-07-01T00:39:55.000000000")
    assert co_data["co"][0] == pytest.approx(0.08788712)
    assert co_data["co_variability"][0] == 0


def test_read_data():
    tb = ThamesBarrier()

    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/ThamesBarrier"
    filename = "thames_test_20190707.csv"

    filepath = os.path.join(dir_path, test_data, filename)

    data = tb.read_data(data_filepath=filepath)

    del data["CH4"]["attributes"]["data_owner"]
    del data["CH4"]["attributes"]["data_owner_email"]

    attributes = {
        "Notes": "~5m above high tide water level, in tidal region of the Thames",
        "inlet_height_magl": "5 m",
        "instrument": "Picarro G2401",
    }

    assert sorted(list(data.keys())) == sorted(["CH4", "CO", "CO2"])
    assert data["CH4"]["attributes"] == attributes
    assert data["CH4"]["metadata"] == {}

    ch4_data = data["CH4"]["data"]

    assert ch4_data.time[0] == pd.Timestamp("2019-07-01T00:39:55.000000000")
    assert ch4_data.time[-1] == pd.Timestamp("2019-08-01T00:10:30.000000000")
    assert ch4_data["CH4"][0] == pytest.approx(1960.8357162)
    assert ch4_data["CH4"][-1] == pytest.approx(2002.003717)
