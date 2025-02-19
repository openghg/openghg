import logging

import pytest
from helpers import (
    attributes_checker_obssurface,
    check_cf_compliance,
    get_surface_datapath,
    parsed_surface_metachecker,
)
from openghg.standardise.surface import parse_noaa
from pandas import Timestamp

mpl_logger = logging.getLogger("matplotlib")
mpl_logger.setLevel(logging.WARNING)

# Disable this for long strings below - Line break occurred before a binary operator (W503)
# flake8: noqa: W503


@pytest.fixture(scope="session")
def scsn06_data():
    filepath = get_surface_datapath(
        filename="ch4_scsn06_surface-flask_1_ccgg_event.txt", source_format="NOAA"
    )

    data = parse_noaa(
        filepath=filepath, site="scsn06", inlet="flask", measurement_type="flask", sampling_period="1200"
    )

    return data


def test_read_obspack_2020():
    '''Test inputs from "obspack_ch4_1_GLOBALVIEWplus_v2.0_2020-04-24"'''
    filepath = get_surface_datapath(
        filename="ch4_esp_surface-flask_2_representative.nc", source_format="NOAA"
    )

    data = parse_noaa(filepath=filepath, site="esp", inlet="flask", measurement_type="flask", network="NOAA")

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
    assert "sampling_period_estimate" in attributes

    ch4_metadata = data["ch4"]["metadata"]

    assert "sampling_period" in ch4_metadata
    assert "sampling_period_estimate" in ch4_metadata

    # Check inlet in metadata
    assert ch4_metadata["inlet"] == "40m"
    assert ch4_metadata["inlet_height_magl"] == "40"


def test_read_obspack_flask_2021():
    '''Test inputs from "obspack_multi-species_1_CCGGSurfaceFlask_v2.0_2021-02-09"'''
    filepath = get_surface_datapath(filename="ch4_spf_surface-flask_1_ccgg_Event.nc", source_format="NOAA")

    data = parse_noaa(filepath=filepath, site="SPF", inlet="flask", measurement_type="flask", network="NOAA")

    # TODO: Replace this test data example when possible.
    # This ObsPack file contains negative heights because SPF is Antarctic Firn Air (ice cores)
    # The intake_height in this case has been used to indicate depth.
    inlet_key = "ch4_-2922m"
    ch4_data = data[inlet_key]["data"]

    assert ch4_data.time[0] == Timestamp("1995-02-01T14:36:00")
    assert ch4_data.time[-1] == Timestamp("2001-01-15T12:34:56")
    assert ch4_data["ch4"][0] == pytest.approx(921.43)
    assert ch4_data["ch4"][-1] == pytest.approx(1143.27)
    assert ch4_data["ch4"].attrs["units"] == "1e-9"
    assert ch4_data["ch4_variability"][0] == pytest.approx(2.71)
    assert ch4_data["ch4_variability"][-1] == pytest.approx(1.4)

    attributes = ch4_data.attrs

    assert "sampling_period" in attributes
    assert attributes["sampling_period"] == "not_set"
    assert "sampling_period_estimate" in attributes
    assert float(attributes["sampling_period_estimate"]) > 0.0
    assert attributes["units"] == "nanomol mol-1"

    ch4_metadata = data[inlet_key]["metadata"]

    assert "sampling_period" in ch4_metadata
    assert "sampling_period_estimate" in ch4_metadata

    # Check inlet in metadata
    assert ch4_metadata["inlet"] == "-2922m"
    assert ch4_metadata["inlet_height_magl"] == "-2922"

    parsed_surface_metachecker(data=data)


def test_read_obspack_tower_multi_height():
    """
    Test inputs from "obspack_multi-species_1_CCGGTowerInsitu_v1.0_2018-02-08".
     - This will contain data at multiple heights (intake_height variable) which should be split.
    """
    filepath = get_surface_datapath(filename="ch4_bao_tower-insitu_1_ccgg_all.nc", source_format="NOAA")

    data = parse_noaa(filepath=filepath, site="BAO", measurement_type="insitu", network="NOAA")

    # Check number of entries extracted - should be three inlet heights: 22m, 100m, 300m
    num_keys = len(data.keys())
    assert num_keys == 3
    assert "ch4_22m" in data.keys()
    assert "ch4_100m" in data.keys()
    assert "ch4_300m" in data.keys()

    # Check values for one of the inlet heights
    inlet_key1 = "ch4_22m"
    ch4_data_22m = data[inlet_key1]["data"]

    assert ch4_data_22m.time[0] == Timestamp("2012-05-04T00:00:00")
    assert ch4_data_22m.time[-1] == Timestamp("2012-07-02T17:00:00")
    assert ch4_data_22m["ch4"][0] == pytest.approx(1969.01)
    assert ch4_data_22m["ch4"][-1] == pytest.approx(2057.103)
    assert ch4_data_22m["ch4_variability"][0] == pytest.approx(78.326)
    assert ch4_data_22m["ch4_variability"][-1] == pytest.approx(18.081)

    # Check metadata for inlet
    metadata = data[inlet_key1]["metadata"]
    assert metadata["inlet"] == "22m"
    assert metadata["inlet_height_magl"] == "22"

    parsed_surface_metachecker(data=data)


def test_read_file_site_filepath_read(scsn06_data):
    ch4_data = scsn06_data["ch4"]["data"]

    assert ch4_data.time[0] == Timestamp("1991-07-05T17:00:00")
    assert ch4_data["ch4"][0] == pytest.approx(1713.21)
    assert ch4_data["ch4_repeatability"][0] == pytest.approx(2.4)
    assert ch4_data["ch4_selection_flag"][0] == 0

    expected_attrs = {
        "station_longitude": 107.0,
        "station_latitude": 6.0,
        "station_long_name": "South China Sea (6 N), N/A",
        "station_height_masl": 15.0,
    }

    attrs = ch4_data.attrs
    for key, value in expected_attrs.items():
        assert attrs[key] == value


@pytest.mark.xfail(reason="broken link to cf conventions")
@pytest.mark.skip_if_no_cfchecker
@pytest.mark.cfchecks
def test_noaa_site_filepath_cf_compliance(scsn06_data):
    ch4_data = scsn06_data["ch4"]["data"]

    assert check_cf_compliance(dataset=ch4_data)


def test_read_raw_file():
    filepath = get_surface_datapath(filename="co_pocn25_surface-flask_1_ccgg_event.txt", source_format="NOAA")

    data = parse_noaa(
        filepath=filepath, inlet="flask", site="pocn25", measurement_type="flask", sampling_period=1200
    )

    co_data = data["co"]["data"]

    assert co_data.time[0] == Timestamp("1990-06-29T05:00:00")
    assert co_data["co"][0] == pytest.approx(94.9)
    assert co_data["co_repeatability"][0] == pytest.approx(-999.99)
    assert co_data["co_selection_flag"][0] == 0

    assert co_data.time[-1] == Timestamp("2017-07-15T04:15:00")
    assert co_data["co"][-1] == pytest.approx(73.16)
    assert co_data["co_repeatability"][-1] == pytest.approx(-999.99)
    assert co_data["co_selection_flag"][-1] == 0


def test_read_incorrect_site_raises():
    filepath = get_surface_datapath(
        filename="ch4_UNKOWN_surface-flask_1_ccgg_event.txt", source_format="NOAA"
    )

    with pytest.raises(ValueError):
        data = parse_noaa(filepath=filepath, site="NotASite", inlet="flask", measurement_type="flask")
