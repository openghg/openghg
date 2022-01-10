import logging
from pandas import Timestamp
import pytest

from helpers import get_datapath, parsed_surface_metachecker
from openghg.standardise.surface import parse_noaa

mpl_logger = logging.getLogger("matplotlib")
mpl_logger.setLevel(logging.WARNING)

# Disable this for long strings below - Line break occurred before a binary operator (W503)
# flake8: noqa: W503


def test_read_obspack_2020():
    '''Test inputs from "obspack_ch4_1_GLOBALVIEWplus_v2.0_2020-04-24"'''
    filepath = get_datapath(filename="ch4_esp_surface-flask_2_representative.nc", data_type="NOAA")

    data = parse_noaa(
        data_filepath=filepath, site="esp", inlet="flask", measurement_type="flask", network="NOAA"
    )

    ch4_data = data["ch4"]["data"]

    assert ch4_data.time[0] == Timestamp("1993-06-17T00:12:30")
    assert ch4_data.time[-1] == Timestamp("2002-01-12T12:00:00")
    assert ch4_data["ch4"][0] == pytest.approx(1.76763e-06)
    assert ch4_data["ch4"][-1] == pytest.approx(1.848995e-06)
    assert ch4_data["ch4_number_of_observations"][0] == 2.0
    assert ch4_data["ch4_number_of_observations"][-1] == 2.0
    assert ch4_data["ch4_variability"][0] == pytest.approx(1.668772e-09)
    assert ch4_data["ch4_variability"][-1] == pytest.approx(1.5202796e-09)

    # Check added attributes around sampling period
    attributes = ch4_data.attrs

    assert "sampling_period" in attributes
    assert attributes["sampling_period"] == "NOT_SET"
    assert "sampling_period_estimate" in attributes

    ch4_metadata = data["ch4"]["metadata"]

    assert "sampling_period" in ch4_metadata
    assert "sampling_period_estimate" in ch4_metadata


@pytest.mark.xfail(reason="Bug: Missing instrment for flask data, required? - see #201")
def test_read_obspack_flask_2021():
    '''Test inputs from "obspack_multi-species_1_CCGGSurfaceFlask_v2.0_2021-02-09"'''
    filepath = get_datapath(filename="ch4_spf_surface-flask_1_ccgg_Event.nc", data_type="NOAA")

    data = parse_noaa(
        data_filepath=filepath, site="SPF", inlet="flask", measurement_type="flask", network="NOAA"
    )

    ch4_data = data["ch4"]["data"]

    assert ch4_data.time[0] == Timestamp("1995-01-28T19:20:00")
    assert ch4_data.time[-1] == Timestamp("2015-12-12T20:15:00")
    assert ch4_data["ch4"][0] == pytest.approx(1673.89)
    assert ch4_data["ch4"][-1] == pytest.approx(1785.86)
    assert ch4_data["ch4_variability"][0] == pytest.approx(2.71)
    assert ch4_data["ch4_variability"][-1] == pytest.approx(0.91)

    attributes = ch4_data.attrs

    assert "sampling_period" in attributes
    assert attributes["sampling_period"] == "NOT_SET"
    assert "sampling_period_estimate" in attributes
    assert float(attributes["sampling_period_estimate"]) > 0.0

    ch4_metadata = data["ch4"]["metadata"]

    assert "sampling_period" in ch4_metadata
    assert "sampling_period_estimate" in ch4_metadata

    parsed_surface_metachecker(data=data)


def test_read_file_site_filename_read():

    filepath = get_datapath(filename="ch4_scsn06_surface-flask_1_ccgg_event.txt", data_type="NOAA")

    data = parse_noaa(
        data_filepath=filepath, site="scsn06", inlet="flask", measurement_type="flask", sampling_period="1200"
    )

    ch4_data = data["ch4"]["data"]

    assert ch4_data.time[0] == Timestamp("1991-07-05T17:00:00")
    assert ch4_data["ch4"][0] == pytest.approx(1713.21)
    assert ch4_data["ch4_repeatability"][0] == pytest.approx(2.4)
    assert ch4_data["ch4_selection_flag"][0] == 0

    metadata = data["ch4"]["metadata"]

    parsed_surface_metachecker(data=data)

    expected_attrs = {
        "station_longitude": 107.0,
        "station_latitude": 6.0,
        "station_long_name": "South China Sea (6 N), N/A",
        "station_height_masl": 15.0,
    }

    for key, value in expected_attrs.items():
        assert ch4_data.attrs[key] == value


def test_read_raw_file():

    filepath = get_datapath(filename="co_pocn25_surface-flask_1_ccgg_event.txt", data_type="NOAA")

    data = parse_noaa(
        data_filepath=filepath, inlet="flask", site="pocn25", measurement_type="flask", sampling_period=1200
    )

    parsed_surface_metachecker(data=data)

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
        "sampling_period": "1200",
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

    filepath = get_datapath(filename="ch4_UNKOWN_surface-flask_1_ccgg_event.txt", data_type="NOAA")

    with pytest.raises(ValueError):
        data = parse_noaa(data_filepath=filepath, site="NotASite", inlet="flask", measurement_type="flask")
