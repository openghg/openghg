import logging

import pytest
from helpers import get_surface_datapath
from openghg.standardise.surface import parse_icos

mpl_logger = logging.getLogger("matplotlib")
mpl_logger.setLevel(logging.WARNING)


def test_read_icos_large_header():
    filepath = get_surface_datapath(filename="ICOS_ATC_L2_L2-2024.1_RGL_90.0_CTS.CH4", source_format="ICOS")

    data = parse_icos(
        filepath=filepath, site="rgl", instrument="g2301", header_type="large", inlet="90m"
    )

    expected_metadata = {
        "site": "rgl",
        "species": "ch4",
        "inlet": "90m",
        "inlet_height_magl": "90",
        "sampling_period": 3600.0,
        "network": "icos",
        "instrument": "g2301",
        "units": "nmol.mol-¹",
        "data_owner_email": "s.odoherty@bris.ac.uk,joseph.pitt@bristol.ac.uk,k.m.stanley@bristol.ac.uk",
        "calibration_scale": "unknown",
        "station_longitude": -2.53992,
        "station_latitude": 51.99747,
        "station_long_name": "Ridge Hill, UK",
        "station_height_masl": 207.0,
    }

    metadata = data["ch4"]["metadata"]

    assert metadata == expected_metadata

    assert data["ch4"]["data"]["ch4"][0] == pytest.approx(2045.79)
    assert data["ch4"]["data"]["ch4"][-1] == pytest.approx(2001.63)
    assert data["ch4"]["data"]["ch4 variability"][0] == pytest.approx(9.801)
    assert data["ch4"]["data"]["ch4 variability"][-1] == pytest.approx(5.185)


def test_read_icos_large_header_incorrect_site_raises():
    filepath = get_surface_datapath(filename="ICOS_ATC_L2_L2-2024.1_RGL_90.0_CTS.CH4", source_format="ICOS")

    with pytest.raises(ValueError):
        parse_icos(
            filepath=filepath,
            site="aaa",
            instrument="g2301",
            header_type="large",
            inlet="90m",
        )


def test_read_icos_large_header_incorrect_instrument_raises():
    filepath = get_surface_datapath(filename="ICOS_ATC_L2_L2-2024.1_RGL_90.0_CTS.CH4", source_format="ICOS")

    with pytest.raises(ValueError):
        parse_icos(
            filepath=filepath,
            site="aaa",
            instrument="sparrow",
            header_type="large",
            inlet="90m",
        )


def test_read_icos_large_header_incorrect_inlet_raises():
    filepath = get_surface_datapath(filename="ICOS_ATC_L2_L2-2024.1_RGL_90.0_CTS.CH4", source_format="ICOS")

    with pytest.raises(ValueError):
        parse_icos(
            filepath=filepath,
            site="aaa",
            instrument="sparrow",
            header_type="large",
            inlet="888m",
        )


def test_read_icos_small_header_file():
    filepath = get_surface_datapath(filename="tta.co2.1minute.222m.min.dat", source_format="ICOS")

    data = parse_icos(
        filepath=filepath,
        site="tta",
        network="ICOS",
        instrument="test_instrument",
        inlet="222m",
        header_type="small",
    )

    attrs = data["co2"]["data"].attrs

    del attrs["file_created"]

    expected_attrs = {
        "conditions_of_use": "Ensure that you contact the data owner at the outset of your project.",
        "source": "In situ measurements of air",
        "Conventions": "CF-1.8",
        "processed_by": "OpenGHG_Cloud",
        "species": "co2",
        "calibration_scale": "unknown",
        "sampling_period": "60.0",
        "sampling_period_unit": "s",
        "station_longitude": -2.98598,
        "station_latitude": 56.55511,
        "station_long_name": "Angus Tower, UK",
        "station_height_masl": 300.0,
    }

    assert attrs == expected_attrs

    co2_data = data["co2"]["data"]

    assert co2_data["co2"][0].values == pytest.approx(401.645)
    assert co2_data["co2 variability"][0].values == pytest.approx(0.087)
    assert co2_data["co2 number_of_observations"][0].values == 13

    co2_metadata = data["co2"]["metadata"]

    expected_metadata = {
        "site": "tta",
        "species": "co2",
        "inlet": "222m",
        "sampling_period": "60.0",
        "network": "icos",
        "instrument": "test_instrument",
        "calibration_scale": "unknown",
        "station_longitude": -2.98598,
        "station_latitude": 56.55511,
        "station_long_name": "Angus Tower, UK",
        "station_height_masl": 300.0,
        "data_type": "surface",
        "source_format": "icos",
    }

    assert co2_metadata == expected_metadata
