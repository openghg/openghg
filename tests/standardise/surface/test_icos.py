import logging
import pytest
from helpers import get_datapath

from openghg.standardise.surface import parse_icos

mpl_logger = logging.getLogger("matplotlib")
mpl_logger.setLevel(logging.WARNING)


def test_read_icos_large_header():
    filepath = get_datapath(filename="mhd.co.hourly.g2401.15m.dat", data_type="ICOS")

    data = parse_icos(
        data_filepath=filepath, species="co", site="mhd", instrument="g2401", header_type="large", inlet="15m"
    )

    expected_metadata = {
        "site": "mhd",
        "species": "co",
        "inlet": "15m",
        "sampling_period": "3600",
        "network": "icos",
        "instrument": "g2401",
        "units": "nmol.mol-¹",
        "calibration_scale": "wmo_co_x2014a",
        "station_longitude": -9.90389,
        "station_latitude": 53.32611,
        "station_long_name": "Mace Head, Ireland",
        "station_height_masl": 5.0,
    }

    metadata = data["co"]["metadata"]

    assert metadata == expected_metadata

    assert data["co"]["data"]["co"][0] == pytest.approx(155.118)
    assert data["co"]["data"]["co"][-1] == pytest.approx(196.383)
    assert data["co"]["data"]["co_variability"][0] == pytest.approx(1.955)
    assert data["co"]["data"]["co_variability"][-1] == pytest.approx(1.924)

    


def test_read_icos_small_header_file():
    filepath = get_datapath(filename="tta.co2.1minute.222m.min.dat", data_type="ICOS")

    data = parse_icos(
        data_filepath=filepath,
        species="co2",
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
        "sampling_period": "60",
        "sampling_period_unit": "s",
        "station_longitude": -2.98598,
        "station_latitude": 56.55511,
        "station_long_name": "Angus Tower, UK",
        "station_height_masl": 300.0,
    }

    assert attrs == expected_attrs

    co2_data = data["co2"]["data"]

    assert co2_data["co2"][0].values == pytest.approx(401.645)
    assert co2_data["co2_variability"][0].values == pytest.approx(0.087)
    assert co2_data["co2_number_of_observations"][0].values == 13

    co2_metadata = data["co2"]["metadata"]

    expected_metadata = {
        "site": "tta",
        "species": "co2",
        "inlet": "222m",
        "sampling_period": "60",
        "network": "icos",
        "instrument": "test_instrument",
        "calibration_scale": "unknown",
        "station_longitude": -2.98598,
        "station_latitude": 56.55511,
        "station_long_name": "Angus Tower, UK",
        "station_height_masl": 300.0,
    }

    assert co2_metadata == expected_metadata
