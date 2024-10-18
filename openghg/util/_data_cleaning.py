from typing import cast

import numpy as np
import pandas as pd
import xarray as xr


def get_averaging_attrs(averaging_period: str) -> dict:
    average_in_seconds = pd.Timedelta(averaging_period).total_seconds()
    result = {"averaged_period": average_in_seconds, "averaged_period_str": averaging_period}
    return result


def mean_resample(ds: xr.Dataset, averaging_period: str, drop_na: bool = False) -> xr.Dataset:
    """Compute mean resampling to averaging period on all variables of dataset."""
    ds_resampled = ds.resample(time=averaging_period).mean(skipna=False, keep_attrs=True)

    ds_resampled.attrs.update(get_averaging_attrs(averaging_period))

    if drop_na is True:
        ds_resampled = ds_resampled.dropna("time")

    return ds_resampled


def weighted_resample(
    ds: xr.Dataset, averaging_period: str, species: str | None = None, drop_na: bool = False
) -> xr.Dataset:
    """Resample concentration and variability, weighted by number of observations."""
    if species is None:
        possible_species = [str(dv) for dv in ds.data_vars if "_" not in str(dv)]
        if len(set(possible_species)) > 1:
            raise ValueError("Could not infer species for weighted resampling; please specify.")
        species = possible_species[0]

    if f"{species}_number_of_observations" not in ds:
        raise ValueError(
            f"Variable `{species}_number_of_observations` not found. Cannot do weighted resample without number of observations."
        )

    # to prevent NaNs from being converted to 0's, we need to skip NaNs and set `min_count ` to 1,
    # so that at least 1 non-NaN value
    # https://github.com/pydata/xarray/issues/4291
    # if dropping NaN, this doesn't matter
    if drop_na is False:
        sum_kwargs = {"skipna": True, "min_count": 1}
    else:
        sum_kwargs = {}

    mf_variability = None
    if f"{species}_variability" in ds:
        mf_variability = ds[f"{species}_variability"]

    result = _weighted_resample(
        mf=ds[species],
        n_obs=ds[f"{species}_number_of_observations"],
        averaging_period=averaging_period,
        species=species,
        mf_variability=mf_variability,
        drop_na=drop_na,
        sum_kwargs=sum_kwargs,
    )

    result.attrs = ds.attrs
    result.attrs.update(get_averaging_attrs(averaging_period))

    return result


def _weighted_resample(
    mf: xr.DataArray,
    n_obs: xr.DataArray,
    averaging_period: str,
    mf_variability: xr.DataArray | None = None,
    species: str | None = None,
    drop_na: bool = False,
    sum_kwargs: dict | None = None,
) -> xr.Dataset:
    """Resample concentration (and variability), weighting by number of observations.

    Resampling to a frequency, say 4h, will give the same result, even if the data is first resampled
    to an intermediate frequency.

    # TODO: make this a doc test?
    For example:

    ds_1h = _weighted_resample(mf, n_obs, "1h", mf_variability)
    ds_4h = _weighted_resample(ds_1h.mf, ds_1h.mf_number_of_observations, "4h", ds_1h.mf_variability)
    ds_4h_2 = _weighted_resample(mf, n_obs, "4h", mf_variability)
    xr.testing.assert_all_close(ds_4h, ds_4h_2)

    Note: to ensure this consistency, you must drop NaN values, or pass kwargs to `sum` to ensure that
    a resampling period containing only NaNs is not resampled to 0.

    For instance: `sum_kwargs = {"skipna": True, "min_count": 1}`

    See https://github.com/pydata/xarray/issues/4291 for more discussion.
    """
    sum_kwargs = sum_kwargs or {}
    sum_kwargs.update({"keep_attrs": True})

    n_obs_resample_sum = n_obs.resample(time=averaging_period).sum(**sum_kwargs)
    weighted_resample_mf = (mf * n_obs).resample(time=averaging_period).sum(**sum_kwargs) / n_obs_resample_sum
    weighted_resample_mf.attrs = mf.attrs

    if species is None:
        species = "mf"

    data_vars = {species: weighted_resample_mf, f"{species}_number_of_observations": n_obs_resample_sum}

    if mf_variability is not None:
        sums_of_squares = n_obs * (mf_variability**2 + mf**2)
        weighted_resample_mf_variability_squared = (
            sums_of_squares.resample(time=averaging_period).sum(**sum_kwargs) / n_obs_resample_sum
            - weighted_resample_mf**2
        )
        weighted_resample_mf_variability = cast(xr.DataArray, np.sqrt(weighted_resample_mf_variability_squared))
        weighted_resample_mf_variability.attrs = mf_variability.attrs
        data_vars[f"{species}_variability"] = weighted_resample_mf_variability

    result = xr.Dataset(data_vars=data_vars)

    if drop_na is True:
        result = result.dropna("time")

    return xr.Dataset(data_vars=data_vars)


def independent_uncertainties_resample(da: xr.DataArray, averaging_period: str, sum_kwargs: dict | None = None, drop_na: bool = False) -> xr.DataArray:
    """Resample uncertainties as the standard deviations of an average of independent quantities.

    We assume that each uncertainty value is the standard deviation of one of the quantities
    being averaged.
    """
    sum_kwargs = sum_kwargs or {}
    n_obs = da.resample(time=averaging_period).count()
    da_resampled_squared = (da**2).resample(time=averaging_period).sum(**sum_kwargs) / n_obs

    result = cast(xr.DataArray, np.sqrt(da_resampled_squared))
    result.attrs = da.attrs

    if drop_na is True:
        result = result.dropna("time")

    return result
