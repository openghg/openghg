import logging

import pytest
from helpers import get_surface_datapath
from openghg.standardise.surface import parse_icos

mpl_logger = logging.getLogger("matplotlib")
mpl_logger.setLevel(logging.WARNING)


def test_read_icos_large_header():
    filepath = get_surface_datapath(filename="mhd.co.hourly.g2401.15m.dat", source_format="ICOS")

    data = parse_icos(
        data_filepath=filepath, site="mhd", instrument="g2401", header_type="large", inlet="15m"
    )

    expected_metadata = {
        "site": "mhd",
        "species": "co",
        "inlet": "15m",
        "sampling_period": "3600.0",
        "network": "icos",
        "instrument": "g2401",
        "units": "nmol.mol-¹",
        "calibration_scale": "wmo_co_x2014a",
        "data_owner_email": "morgan.lopez@lsce.ipsl.fr",
    }

    metadata = data["co"]["metadata"]

    assert metadata == expected_metadata

    assert data["co"]["data"]["co"][0] == pytest.approx(155.118)
    assert data["co"]["data"]["co"][-1] == pytest.approx(196.383)
    assert data["co"]["data"]["co variability"][0] == pytest.approx(1.955)
    assert data["co"]["data"]["co variability"][-1] == pytest.approx(1.924)


def test_read_icos_large_header_incorrect_site_raises():
    filepath = get_surface_datapath(filename="mhd.co.hourly.g2401.15m.dat", source_format="ICOS")

    with pytest.raises(ValueError):
        parse_icos(
            data_filepath=filepath,
            site="aaa",
            instrument="g2401",
            header_type="large",
            inlet="15m",
        )


def test_read_icos_large_header_incorrect_instrument_raises():
    filepath = get_surface_datapath(filename="mhd.co.hourly.g2401.15m.dat", source_format="ICOS")

    with pytest.raises(ValueError):
        parse_icos(
            data_filepath=filepath,
            site="aaa",
            instrument="sparrow",
            header_type="large",
            inlet="15m",
        )


def test_read_icos_large_header_incorrect_inlet_raises():
    filepath = get_surface_datapath(filename="mhd.co.hourly.g2401.15m.dat", source_format="ICOS")

    with pytest.raises(ValueError):
        parse_icos(
            data_filepath=filepath,
            site="aaa",
            instrument="sparrow",
            header_type="large",
            inlet="888m",
        )


def test_read_icos_small_header_file():
    filepath = get_surface_datapath(filename="tta.co2.1minute.222m.min.dat", source_format="ICOS")

    data = parse_icos(
        data_filepath=filepath,
        site="tta",
        network="ICOS",
        instrument="test_instrument",
        inlet="222m",
        header_type="small",
    )

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
        "data_type": "surface",
        "source_format": "icos",
    }

    assert co2_metadata == expected_metadata
