import logging

import pandas as pd
import pytest
from helpers import check_cf_compliance, get_surface_datapath, parsed_surface_metachecker
from openghg.standardise.surface import parse_gcwerks_nc

mpl_logger = logging.getLogger("matplotlib")
mpl_logger.setLevel(logging.WARNING)


@pytest.fixture(scope="session")
def thd_data():
    thd_path = get_surface_datapath(
        filename="agage_thd_cfc-11_20240513-test.nc", source_format="GC_nc"
    )

    gas_data = parse_gcwerks_nc(data_filepath=thd_path, site="THD", instrument="gcmd", network="agage",)

    return gas_data


@pytest.fixture(scope="session")
def cgo_data():
    cgo_data = get_surface_datapath(
        filename="agage_cgo_hcfc-133a_20240513-test.nc", source_format="GC_nc"
    )

    gas_data = parse_gcwerks_nc(data_filepath=cgo_data, site="cgo", instrument="medusa", network="agage",)

    return gas_data


def test_read_file_capegrim(cgo_data):
    parsed_surface_metachecker(data=cgo_data)

    # Expect a single label at 70m in this test file, since only one height in the period convered
    expected_keys = ["hcfc133a_70m"]

    sorted_keys = sorted(list(cgo_data.keys()))

    assert sorted_keys[:2] == expected_keys


def test_read_file_thd():
    thd_path = get_surface_datapath(
        filename="agage_thd_cfc-11_20240513-test.nc", source_format="GC_nc"
    )

    gas_data = parse_gcwerks_nc(
        data_filepath=thd_path,
        site="thd",
        network="agage",
        instrument="gcmd",
        sampling_period="1",  # Checking this can be compared successfully
    )

    parsed_surface_metachecker(data=gas_data)

    expected_key = ["cfc11_15m"]

    assert sorted(list(gas_data.keys())) == expected_key

    meas_data = gas_data["cfc11_15m"]["data"]

    assert meas_data.time[0] == pd.Timestamp("1995-09-30T17:22:00")
    assert meas_data.time[-1] == pd.Timestamp("1995-11-13T21:38:00")

    assert meas_data["cfc11"][0].values.item() == 267.0292663574219
    assert meas_data["cfc11"][-1].values.item() == 266.9176025390625


@pytest.mark.skip_if_no_cfchecker
@pytest.mark.cfchecks
def test_gc_thd_cf_compliance(thd_data):
    meas_data = thd_data["cfc11_10m"]["data"]
    assert check_cf_compliance(dataset=meas_data)


def test_read_invalid_instrument_raises():
    thd_path = get_surface_datapath(
        filename="agage_thd_cfc-11_20240513-test.nc", source_format="GC_nc"
    )

    with pytest.raises(ValueError):
        parse_gcwerks_nc(
            data_filepath=thd_path, site="THD", instrument="fish", network="agage",
        )


def test_expected_metadata_thd_cfc11():
    cfc11_path = get_surface_datapath(filename="agage_thd_cfc-11_20240513-test.nc", source_format="GC_nc")

    data = parse_gcwerks_nc(data_filepath=cfc11_path, site="THD", network="agage", instrument="gcmd")

    metadata = data["cfc11_15m"]["metadata"]

    expected_metadata = {
        "data_type": "surface",
        "instrument": "gcmd",
        "site": "THD",
        "network": "agage",
        "sampling_period": "1.0",
        "units": "1e-12",
        "calibration_scale": "SIO-05",
        "inlet": "15m",
        "species": "cfc11",
        "inlet_height_magl": 15.0,
        "data_owner": "Ray F. Weiss, Jens Muhle",
        "data_owner_email": "rfweiss@ucsd.edu, jmuhle@ucsd.edu",
        "station_longitude": -124.151,
        "station_latitude": 41.0541,
        "station_height_masl": 107.0,
        "station_long_name": "Trinidad Head, California",
    }

    assert metadata == expected_metadata
