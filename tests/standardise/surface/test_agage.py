import logging

import pandas as pd
import pytest
from helpers import check_cf_compliance, get_surface_datapath, parsed_surface_metachecker
from openghg.standardise.surface import parse_agage

mpl_logger = logging.getLogger("matplotlib")
mpl_logger.setLevel(logging.WARNING)


@pytest.fixture(scope="session")
def thd_data():
    thd_path = get_surface_datapath(filename="agage_thd_cfc-11_20240703-test.nc", source_format="GC_nc")

    gas_data = parse_agage(
        filepath=thd_path,
        site="THD",
        instrument="gcmd",
        network="agage",
    )

    return gas_data


@pytest.fixture(scope="session")
def cgo_data():
    cgo_data = get_surface_datapath(
        filename="agage_cgo_hcfc-133a_20240703-multi-instru-test.nc", source_format="GC_nc"
    )

    gas_data = parse_agage(
        filepath=cgo_data,
        site="cgo",
        instrument="GCMS-Medusa/GCMS",
        network="agage",
    )

    return gas_data


def test_read_file_capegrim(cgo_data):
    # Expect two labels at 70m and 80m in this test file, since multiple heights in the period convered
    expected_keys = ["hcfc133a_70m", "hcfc133a_80m"]

    sorted_keys = sorted(list(cgo_data.keys()))

    assert sorted_keys[:2] == expected_keys


def test_read_file_thd():
    thd_path = get_surface_datapath(filename="agage_thd_cfc-11_20240703-test.nc", source_format="GC_nc")

    gas_data = parse_agage(
        filepath=thd_path,
        site="thd",
        network="agage",
        instrument="gcmd",
        sampling_period="1",  # Checking this can be compared successfully
    )

    expected_key = ["cfc11_15m"]

    assert sorted(list(gas_data.keys())) == expected_key

    meas_data = gas_data["cfc11_15m"]["data"]

    assert meas_data.time[0] == pd.Timestamp("1995-09-30T17:22:00")
    assert meas_data.time[-1] == pd.Timestamp("1995-11-13T21:38:00")

    assert meas_data["cfc11"][0].values.item() == 267.0292663574219
    assert meas_data["cfc11"][-1].values.item() == 266.9176025390625


@pytest.mark.xfail(reason="broken link to cf conventions")
@pytest.mark.skip_if_no_cfchecker
@pytest.mark.cfchecks
def test_gc_thd_cf_compliance(thd_data):
    meas_data = thd_data["cfc11_15m"]["data"]
    assert check_cf_compliance(dataset=meas_data)


def test_read_invalid_instrument_raises():
    thd_path = get_surface_datapath(filename="agage_thd_cfc-11_20240703-test.nc", source_format="GC_nc")

    with pytest.raises(ValueError):
        parse_agage(
            filepath=thd_path,
            site="THD",
            instrument="fish",
            network="agage",
        )


def test_expected_metadata_thd_cfc11():
    cfc11_path = get_surface_datapath(filename="agage_thd_cfc-11_20240703-test.nc", source_format="GC_nc")

    data = parse_agage(filepath=cfc11_path, site="THD", network="agage", instrument="gcmd")

    metadata = data["cfc11_15m"]["metadata"]

    expected_metadata = {
        "data_type": "surface",
        "instrument": "gcmd",
        "instrument_name_0": "gcmd",
        "site": "THD",
        "network": "agage",
        "sampling_period": "1.0",
        "units": "1e-12",
        "calibration_scale": "SIO-05",
        "inlet": "15m",
        "species": "cfc11",
        "inlet_height_magl": 15.0,
    }

    assert metadata == expected_metadata


def test_instrument_metadata(cgo_data):
    """
    This test checks for instrument and instrument_name_number metadata.
    """
    assert cgo_data["hcfc133a_70m"]["metadata"]["instrument_name_0"] == "agilent_5975"
    assert cgo_data["hcfc133a_70m"]["metadata"]["instrument"] == "multiple"
    assert cgo_data["hcfc133a_70m"]["metadata"]["instrument_name_1"] == "agilent_5973"
