import logging

import pandas as pd
import pytest
from helpers import check_cf_compliance, get_surface_datapath, parsed_surface_metachecker
from openghg.standardise.surface import parse_gcwerks_nc

mpl_logger = logging.getLogger("matplotlib")
mpl_logger.setLevel(logging.WARNING)

# TODO: 
# Choose relevant data to load in and test
# add to the data testing directory
# decide if any of the tests below need removing 
# decide if any new tests are needed to test .nc-specific bits
# rewrite existing tests to make sense for .nc

@pytest.fixture(scope="session")
def thd_data():
    thd_path = get_surface_datapath(filename="AGAGE-GCMD_THD_cfc-11.nc", source_format="GC_nc")

    gas_data = parse_gcwerks_nc(
        data_filepath=thd_path,
        site="THD",
        instrument="medusa",
        network="agage",
    )

    return gas_data


@pytest.fixture(scope="session")
def cgo_data():
    cgo_data = get_surface_datapath(filename="AGAGE-GCMS-MEDUSA_CGO_hcfc-133a.nc", source_format="GC_nc")

    gas_data = parse_gcwerks_nc(
        data_filepath=cgo_data,
        site="cgo",
        instrument="gcms",
        network="agage",
    )

    return gas_data


def test_read_file_capegrim(cgo_data):
    parsed_surface_metachecker(data=cgo_data)

    # Expect two labels, corresponding to the two heights found in process_gcwerks_parameters.json
    expected_keys = ['hcfc133a_10m',
                     'hcfc133a_70m']

    sorted_keys = sorted(list(cgo_data.keys()))

    assert sorted_keys[:2] == expected_keys

def test_read_file_thd():
    thd_path = get_surface_datapath(filename="AGAGE-GCMD_THD_cfc-11.nc", source_format="GC_nc")

    gas_data = parse_gcwerks_nc(
        data_filepath=thd_path,
        site="thd",
        network="agage",
        instrument="gcmd",
        sampling_period="75",  # Checking this can be compared successfully
    )

    parsed_surface_metachecker(data=gas_data)

    expected_key = ['cfc11_10m']

    assert sorted(list(gas_data.keys())) == expected_key

    meas_data = gas_data["cfc11_10m"]["data"]

    assert meas_data.time[0] == pd.Timestamp("1995-09-30T17:21:22.5")
    assert meas_data.time[-1] == pd.Timestamp("2022-12-31T23:06:22.5")

    assert meas_data["cfc11"][0].values.item() == 267.0292663574219
    assert meas_data["cfc11"][-1] == 218.2406005859375

@pytest.mark.skip_if_no_cfchecker
@pytest.mark.cfchecks
def test_gc_thd_cf_compliance(thd_data):
    meas_data = thd_data["cfc11_10m"]["data"]
    assert check_cf_compliance(dataset=meas_data)


def test_read_invalid_instrument_raises():
    thd_path = get_surface_datapath(filename="trinidadhead.01.C", source_format="GC")

    with pytest.raises(ValueError):
        parse_gcwerks_nc(
            data_filepath=thd_path,
            site="CGO",
            instrument="fish",
            network="agage",
        )


@pytest.mark.skip_if_no_cfchecker
@pytest.mark.cfchecks
def test_thd_cf_compliance(thd_data):
    meas_data = thd_data["ch4_10m"]["data"]
    assert check_cf_compliance(dataset=meas_data)

    # expected_metadata = {
    #     "instrument": "gcmd",
    #     "site": "thd",
    #     "network": "agage",
    #     "species": "ch4",
    #     "units": "ppb",
    #     "scale": "Tohoku",
    #     "inlet": "10m",
    # }

    # metadata = res["ch4_10m"]["metadata"]

    # assert metadata == expected_metadata

    # data = res["ch4_10m"]["data"]

    # assert data.time[0] == pd.Timestamp("2001-01-01T01:05:22.5")
    # assert data.time[-1] == pd.Timestamp("2001-01-01T10:25:22.5")
    # assert data["ch4"][0] == pytest.approx(1818.62)
    # assert data["ch4"][-1] == pytest.approx(1840.432)
