import numpy as np
import pandas as pd
import pytest
import xarray as xr

from openghg.data_processing._resampling import (
    _surface_obs_resampler_dict,
    mean_resample,
    weighted_resample,
    uncorrelated_errors_resample,
    surface_obs_resampler,
)


rng = np.random.default_rng(seed=196883)


def big_randoms(n):
    return rng.normal(100, 2, n)


def small_randoms(n):
    return rng.normal(5, 0.5, n)


def binary_randoms(n):
    return rng.binomial(1, 0.5, n)


def int_randoms(n):
    return rng.integers(14, 19, n)


@pytest.fixture
def mhd_ds():
    """Make dataset with number of obs and variability"""
    times = pd.date_range("2019-01-01", "2019-01-08", freq="1h", inclusive="left")
    n = len(times)
    ch4 = big_randoms(n)
    ch4_repeatability = small_randoms(n)
    integration_flag = binary_randoms(n)
    status_flag = binary_randoms(n)

    ds = xr.Dataset(
        data_vars={
            "ch4": (["time"], ch4),
            "ch4_repeatability": (["time"], ch4_repeatability),
            "status_flag": (["time"], status_flag),
            "integration_flag": (["time"], integration_flag),
        },
        coords={"time": (["time"], times)},
    )
    ds.attrs = {
        "author": "Arthur P. Scientist",
        "Calibration_scale": "TU1987",
        "Conventions": "CF-1.8",
        "station_long_name": "Mace Head, Ireland",
    }
    ds.ch4.attrs = {"long_name": "mole fraction of methane in air", "units": "1e-9"}
    ds.ch4_repeatability.attrs = {"long_name": "repeatability of mole fraction of methane in air", "units": "1e-9"}
    return ds


@pytest.fixture
def tac_ds():
    """Make dataset with repeatability."""
    times = pd.date_range("2019-01-01", "2019-01-08", freq="1h", inclusive="left")
    n = len(times)
    ch4 = big_randoms(n)
    ch4_variability = small_randoms(n)
    ch4_number_of_observations = int_randoms(n)

    ds = xr.Dataset(
        data_vars={
            "ch4": (["time"], ch4),
            "ch4_variability": (["time"], ch4_variability),
            "ch4_number_of_observations": (["time"], ch4_number_of_observations),
        },
        coords={"time": (["time"], times)},
    )
    ds.attrs = {
        "author": "Aretha Q. Scientist",
        "Calibration_scale": "xmo-x2004a",
        "Conventions": "CF-1.8",
        "station_long_name": "Tacolneston Tower, UK",
    }
    ds.ch4.attrs = {"long_name": "mole fraction of methane in air", "units": "1e-9"}
    ds.ch4_variability.attrs = {"long_name": "variability of mole fraction of methane in air", "units": "1e-9"}
    ds.ch4_number_of_observations.attrs = {"long_name": "number of observations of mole fraction of methane in air"}
    return ds


def test_mean_resample(tac_ds):
    ds = xr.ones_like(tac_ds)

    ds_4h = mean_resample(ds, averaging_period="4h")

    assert {"averaged_period": 4 * 3600.0, "averaged_period_str": "4h"}.items() <= ds_4h.attrs.items()

    ds_4h_expected = xr.ones_like(ds.resample(time="4h").mean(keep_attrs=True))

    xr.testing.assert_equal(ds_4h, ds_4h_expected)


def test_weighted_resample(tac_ds):
    """Weighted resample should give the same result if a resampling is done in two steps or one."""
    ds_4h = weighted_resample(tac_ds, averaging_period="4h", species="ch4")
    ds_4h_12h = weighted_resample(ds_4h, averaging_period="12h", species="ch4")

    ds_12h = weighted_resample(tac_ds, averaging_period="12h", species="ch4")

    # check that values are close
    xr.testing.assert_allclose(ds_4h_12h, ds_12h)

    assert ds_4h_12h.attrs == ds_12h.attrs


def test_weighted_resample_with_nans(tac_ds):
    """NaN values shouldn't affect the final result of weighted resample."""
    tac_ds = tac_ds.assign(ch4=tac_ds.ch4.where(tac_ds.time >= pd.to_datetime("2019-01-01 10:00:00"), other=np.nan))

    ds_4h = weighted_resample(tac_ds, averaging_period="4h", species="ch4")
    ds_4h_12h = weighted_resample(ds_4h, averaging_period="12h", species="ch4")
    ds_12h = weighted_resample(tac_ds, averaging_period="12h", species="ch4")

    # check that values are close
    xr.testing.assert_allclose(ds_4h_12h, ds_12h)

    assert ds_4h_12h.attrs == ds_12h.attrs


def test_make_surface_obs_resampler_dict(mhd_ds, tac_ds):
    resampler_dict1 = _surface_obs_resampler_dict(mhd_ds, species="ch4")
    resampler_dict2 = _surface_obs_resampler_dict(tac_ds, species="ch4")

    assert resampler_dict1 == {
        "uncorrelated_errors": ["ch4_repeatability"],
        "variability": ["ch4"],
        "mean": ["ch4"],
    }
    assert resampler_dict2 == {"weighted": ["ch4", "ch4_number_of_observations", "ch4_variability"]}


def test_surface_obs_resampling_with_repeatability(mhd_ds):
    result = surface_obs_resampler(mhd_ds, averaging_period="4h", species="ch4")

    expected_repeatability = uncorrelated_errors_resample(
        mhd_ds.ch4_repeatability, averaging_period="4h"
    )
    xr.testing.assert_allclose(result.ch4_repeatability, expected_repeatability)

    expected_others = mean_resample(mhd_ds.drop_vars("ch4_repeatability"), averaging_period="4h")
    xr.testing.assert_allclose(result.drop_vars(["ch4_repeatability", "ch4_variability"]), expected_others)

    assert result.ch4.attrs == mhd_ds.ch4.attrs
    assert result.ch4_repeatability.attrs == mhd_ds.ch4_repeatability.attrs
    assert result.ch4_variability.attrs == {"long_name": "mole fraction of methane in air_variability", "units": "1e-9"}


def test_surface_obs_resampling_with_variability(tac_ds):
    result = surface_obs_resampler(tac_ds, averaging_period="4h", species="ch4")

    expected = weighted_resample(tac_ds, averaging_period="4h", species="ch4",)
    xr.testing.assert_allclose(result, expected)


@pytest.mark.parametrize(("func", "func_kwargs"),
                         [
                             (mean_resample, {}),
                             (weighted_resample, {"species": "ch4"}),
                             (surface_obs_resampler, {"species": "ch4"}),
                         ])
def test_attributes_kept(func, func_kwargs, tac_ds):
    resampled_ds = func(tac_ds, "4h", **func_kwargs)

    assert tac_ds.attrs.items() <= resampled_ds.attrs.items()
