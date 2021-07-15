import logging
import os

from pathlib import Path
from pandas import Timestamp
import pytest

from openghg.modules import NOAA, Datasource
from openghg.objectstore import get_local_bucket
from helpers import get_datapath

mpl_logger = logging.getLogger("matplotlib")
mpl_logger.setLevel(logging.WARNING)

# Disable this for long strings below - Line break occurred before a binary operator (W503)
# flake8: noqa: W503


def test_read_obspack():
    noaa = NOAA()

    filepath = get_datapath(filename="ch4_esp_surface-flask_2_representative.nc", data_type="NOAA")

    data = noaa.read_file(data_filepath=filepath, site="esp", inlet="flask", measurement_type="flask", network="NOAA")

    ch4_data = data["ch4"]["data"]

    assert ch4_data.time[0] == Timestamp("1993-06-17T00:12:30")
    assert ch4_data.time[-1] == Timestamp("2002-01-12T12:00:00")
    assert ch4_data["value"][0] == pytest.approx(1.76763e-06)
    assert ch4_data["value"][-1] == pytest.approx(1.848995e-06)
    assert ch4_data["nvalue"][0] == 2.0
    assert ch4_data["nvalue"][-1] == 2.0
    assert ch4_data["value_std_dev"][0] == pytest.approx(1.668772e-09)
    assert ch4_data["value_std_dev"][-1] == pytest.approx(1.5202796e-09)


def test_read_file_site_filename_read():
    noaa = NOAA()

    filepath = get_datapath(filename="ch4_scsn06_surface-flask_1_ccgg_event.txt", data_type="NOAA")

    data = noaa.read_file(data_filepath=filepath, site="scsn06", inlet="flask", measurement_type="flask", sampling_period="1200")

    ch4_data = data["ch4"]["data"]

    assert ch4_data.time[0] == Timestamp("1991-07-05T17:00:00")
    assert ch4_data["ch4"][0] == pytest.approx(1713.21)
    assert ch4_data["ch4_repeatability"][0] == pytest.approx(2.4)
    assert ch4_data["ch4_selection_flag"][0] == 0

    metadata = data["ch4"]["metadata"]

    expected_metadata = {
        "species": "ch4",
        "site": "SCSN06",
        "measurement_type": "flask",
        "network": "NOAA",
        "inlet": "flask",
        "sampling_period": "1200",
    }

    assert metadata == expected_metadata

    expected_attrs = {
        "data_owner": "Ed Dlugokencky, Gabrielle Petron (CO)",
        "data_owner_email": "ed.dlugokencky@noaa.gov, gabrielle.petron@noaa.gov",
        "inlet_height_magl": "NA",
        "instrument": "GC-FID",
        "Conditions of use": "Ensure that you contact the data owner at the outset of your project.",
        "Source": "In situ measurements of air",
        "Conventions": "CF-1.6",
        "Processed by": "OpenGHG_Cloud",
        "species": "ch4",
        "Calibration_scale": "unknown",
        "station_longitude": 107.0,
        "station_latitude": 6.0,
        "station_long_name": "South China Sea (6 N), N/A",
        "station_height_masl": 15.0,
    }

    del ch4_data.attrs["File created"]

    assert ch4_data.attrs == expected_attrs


def test_read_raw_file():
    noaa = NOAA()

    filepath = get_datapath(filename="co_pocn25_surface-flask_1_ccgg_event.txt", data_type="NOAA")

    data = noaa.read_file(data_filepath=filepath, inlet="flask", site="pocn25", measurement_type="flask", sampling_period=1200)

    assert data["co"]["metadata"] == {
        "species": "co",
        "site": "POC",
        "measurement_type": "flask",
        "network": "NOAA",
        "inlet": "flask",
        "sampling_period": "1200",
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
        "Processed by": "OpenGHG_Cloud",
        "species": "co",
        "Calibration_scale": "unknown",
        "station_longitude": -139.0,
        "station_latitude": 25.0,
        "station_long_name": "Pacific Ocean (25 N), N/A",
        "station_height_masl": 10.0,
    }

    del attrs["File created"]

    assert attrs == expected_attrs


def test_read_incorrect_site_raises():
    noaa = NOAA()

    filepath = get_datapath(filename="ch4_UNKOWN_surface-flask_1_ccgg_event.txt", data_type="NOAA")

    with pytest.raises(ValueError):
        data = noaa.read_file(data_filepath=filepath, site="NotASite", inlet="flask", measurement_type="flask")
