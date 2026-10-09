import logging

import numpy as np
import pandas as pd
import pytest
import xarray as xr
from helpers import check_cf_compliance, get_surface_datapath
from openghg.standardise.surface import parse_agage

mpl_logger = logging.getLogger("matplotlib")
mpl_logger.setLevel(logging.WARNING)


@pytest.fixture(scope="session")
def thd_data():
    thd_path = get_surface_datapath(
        filename="agage-private_thd_cfc-11_20260113-test.nc", source_format="GC_nc"
    )

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
    thd_path = get_surface_datapath(
        filename="agage-private_thd_cfc-11_20260113-test.nc", source_format="GC_nc"
    )

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
    assert meas_data.time[-1] == pd.Timestamp("2025-12-31T23:18:00")

    assert meas_data["cfc11"][0].values.item() == 267.0292663574219
    assert meas_data["cfc11"][-1].values.item() == 211.28778076171875


@pytest.mark.xfail(reason="broken link to cf conventions")
@pytest.mark.skip_if_no_cfchecker
@pytest.mark.cfchecks
def test_gc_thd_cf_compliance(thd_data):
    meas_data = thd_data["cfc11_15m"]["data"]
    assert check_cf_compliance(dataset=meas_data)


def test_read_invalid_instrument_raises():
    thd_path = get_surface_datapath(
        filename="agage-private_thd_cfc-11_20260113-test.nc", source_format="GC_nc"
    )

    with pytest.raises(ValueError):
        parse_agage(
            filepath=thd_path,
            site="THD",
            instrument="fish",
            network="agage",
        )


@pytest.mark.parametrize(
    "species_attr", [pytest.param(None, id="missing"), pytest.param(123, id="non-string")]
)
def test_missing_species_attribute_raises(species_attr):
    attrs = {"instrument_type": "gcmd", "instrument": "gcmd"}
    if species_attr is not None:
        attrs["species"] = species_attr
    dataset = xr.Dataset(attrs=attrs)

    with pytest.raises(ValueError, match="No 'species' attribute found"):
        parse_agage(data=dataset, site="THD", network="agage")


def test_read_variabilities():
    """
    Check that if an AGAGE file has a mf_variability variable, it is read in
    """
    cgo_path = get_surface_datapath(
        filename="agage_cgo_cfc-11_20250704-test-variabilities.nc", source_format="GC_nc"
    )

    data = parse_agage(
        filepath=cgo_path,
        site="cgo",
        instrument="GCMD",
        network="agage",
    )

    assert "cfc11_variability" in data["cfc11_70m"]["data"].variables


def test_expected_metadata_thd_cfc11():
    cfc11_path = get_surface_datapath(
        filename="agage-private_thd_cfc-11_20260113-test.nc", source_format="GC_nc"
    )

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


@pytest.mark.parametrize("all_missing", [False, True], ids=["partial", "all-missing"])
def test_missing_mole_fractions_drop_without_removing_missing_uncertainties(all_missing):
    """Remove unavailable mole fractions while preserving finite rows with missing errors.

    An inlet with no remaining mole fractions is skipped; an entirely missing
    species retains the existing parser error rather than producing empty data.
    """
    dataset = xr.Dataset(
        {
            "mf": ("time", [np.nan, 1.0, 2.0, np.nan], {"units": "1e-12"}),
            "mf_repeatability": ("time", [0.1, np.nan, 0.2, 0.1]),
            "mf_variability": ("time", [0.3, 0.4, np.nan, 0.3]),
            "inlet_height": ("time", [15.0, 15.0, 15.0, 25.0]),
            "sampling_period": ("time", [1.0] * 4),
        },
        coords={"time": pd.date_range("2020-01-01", periods=4, freq="h")},
        attrs={
            "species": "cfc-11",
            "instrument_type": "gcmd",
            "instrument": "gcmd",
            "calibration_scale": "SIO-05",
        },
    )
    if all_missing:
        dataset["mf"] = xr.full_like(dataset.mf, np.nan)
        with pytest.raises(ValueError, match="All values for this species cfc11 is null"):
            parse_agage(data=dataset, site="THD", network="agage")
        return

    result = parse_agage(data=dataset, site="THD", network="agage")

    assert set(result) == {"cfc11_15m"}
    observations = result["cfc11_15m"]["data"]
    np.testing.assert_array_equal(observations.time, dataset.time.isel(time=[1, 2]))
    np.testing.assert_array_equal(observations.cfc11, [1.0, 2.0])
    np.testing.assert_allclose(observations.cfc11_repeatability, [np.nan, 0.2], equal_nan=True)
    np.testing.assert_allclose(observations.cfc11_variability, [0.4, np.nan], equal_nan=True)
