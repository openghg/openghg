from functools import partial
from typing import Callable, cast, overload

import numpy as np
import pandas as pd
import xarray as xr

from ._function_inputs import split_function_inputs


functions = {}


def register(func: Callable) -> Callable:
    functions[func.__name__] = func
    return func


def get_averaging_attrs(averaging_period: str) -> dict:
    average_in_seconds = pd.Timedelta(averaging_period).total_seconds()
    result = {"averaged_period": average_in_seconds, "averaged_period_str": averaging_period}
    return result


def add_averaging_attrs(ds: xr.Dataset, averaging_period: str) -> xr.Dataset:
    """Add "averaged_period" and "averaged_period_str" attributes to dataset.

    We don't need to return the dataset here, but it allows this function to fit
    in a pipeline of other post-processing functions.
    """
    ds.attrs.update(get_averaging_attrs(averaging_period))
    return ds


@register
def mean_resample(ds: xr.Dataset, averaging_period: str, drop_na: bool = False) -> xr.Dataset:
    """Compute mean resampling to averaging period on all variables of dataset."""
    ds_resampled = ds.resample(time=averaging_period).mean(skipna=False, keep_attrs=True)

    ds_resampled.attrs.update(get_averaging_attrs(averaging_period))

    if drop_na is True:
        ds_resampled = ds_resampled.dropna("time")

    return ds_resampled


def _guess_species(ds: xr.Dataset) -> str:
    """Rough guess at species name from surface data variables."""
    possible_species = [str(dv) for dv in ds.data_vars if "_" not in str(dv)]

    if len(set(possible_species)) > 1:
        raise ValueError("Could not infer species for weighted resampling; please specify.")

    return possible_species[0]


@register
def weighted_resample(
    ds: xr.Dataset, averaging_period: str, species: str | None = None, drop_na: bool = False
) -> xr.Dataset:
    """Resample concentration and variability, weighted by number of observations."""
    if species is None:
        species = _guess_species(ds)

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
        weighted_resample_mf_variability = cast(
            xr.DataArray, np.sqrt(weighted_resample_mf_variability_squared)
        )
        weighted_resample_mf_variability.attrs = mf_variability.attrs
        data_vars[f"{species}_variability"] = weighted_resample_mf_variability

    result = xr.Dataset(data_vars=data_vars)

    if drop_na is True:
        result = result.dropna("time")

    return xr.Dataset(data_vars=data_vars)


@overload
def independent_uncertainties_resample(
    data: xr.DataArray, averaging_period: str, sum_kwargs: dict | None = None, drop_na: bool = False
) -> xr.DataArray: ...


@overload
def independent_uncertainties_resample(
    data: xr.Dataset, averaging_period: str, sum_kwargs: dict | None = None, drop_na: bool = False
) -> xr.Dataset: ...


@register
def independent_uncertainties_resample(
    data: xr.DataArray | xr.Dataset,
    averaging_period: str,
    sum_kwargs: dict | None = None,
    drop_na: bool = False,
) -> xr.DataArray | xr.Dataset:
    """Resample uncertainties as the standard deviations of an average of independent quantities.

    We assume that each uncertainty value is the standard deviation of one of the quantities
    being averaged.
    """
    sum_kwargs = sum_kwargs or {}

    if isinstance(data, xr.Dataset):
        # apply indepedent uncertainties resample to each data variable, but don't drop NaN yet
        func = partial(
            independent_uncertainties_resample,
            averaging_period=averaging_period,
            sum_kwargs=sum_kwargs,
            drop_na=False,
        )
        result = data.map(func)
        result.attrs.update(get_averaging_attrs(averaging_period))
    else:
        n_obs = data.resample(time=averaging_period).count()
        data_resampled_squared = (data**2).resample(time=averaging_period).sum(**sum_kwargs) / n_obs

        result = cast(xr.DataArray, np.sqrt(data_resampled_squared))

    result.attrs = data.attrs

    if drop_na is True:
        result = result.dropna("time")

    return result


def apply_funcs(
    ds: xr.Dataset,
    funcs: list,
    func_vars: list[list[str]],
    drop_remaining_vars: bool = False,
    remainder_func: Callable | None = None,
    keep_attrs: bool = True,
) -> xr.Dataset:
    """Apply functions to the data variables of a dataset.

    For each function, there is a corresponding list of variables that the function will be
    applied to.

    Variables not in any list in `func_vars` is a "remaining" variable. These can be dropped
    or passed through unchanged; alternatively, `remainder_func` can be applied to all remaining
    variables.

    Note: it is possible for a data variables to appear in more than one list in `func_vars`; this
    variable will have multiple functions applied to it.

    Args:
        ds: dataset to apply functions to
        funcs: list of functions to apply to certain data variables of dataset
        func_vars: list of lists of data variables; the first function in `funcs` will be applied to
            the first list of data variables in `func_vars`, and so on.
        drop_remaining_vars: if True, drop any variables not appearing in `func_vars`. If False,
            pass remaining variables through unchanged, or pass them to `remainder_func`, if it is
            specified.
        remainder_func: if specified and `drop_remaining_vars` is False, this function will be applied
            to any variables not in a list in `func_vars`.
        keep_attrs: if True, make global attributes of output dataset the same as the global attributes
            of input dataset.

    Returns:
        dataset with functions applied to specified variables.
    """
    if len(funcs) != len(func_vars):
        msg = (
            f"Specified {len(funcs)} functions and {len(func_vars)} lists of function variables. "
            "Each function must has its own list of function variables."
        )
        raise ValueError(msg)

    if drop_remaining_vars is False:
        all_func_vars = [x for y in func_vars for x in y]
        remaining_vars = [str(dv) for dv in ds.data_vars if dv not in all_func_vars]
    else:
        remaining_vars = []

    results = []

    for func, fvars in zip(funcs, func_vars):
        results.append(func(ds[fvars]))

    if remaining_vars:
        if remainder_func is not None:
            results.append(ds[remaining_vars].map(remainder_func))
        else:
            results.append(ds[remaining_vars])

    result = xr.merge(results)

    if keep_attrs:
        result.attrs = ds.attrs

    return result


def resampler(
    ds: xr.Dataset,
    func_dict: dict[str, list[str]],
    averaging_period: str,
    drop_na: bool = True,
    apply_func_kwargs: dict | None = None,
    **kwargs,
) -> xr.Dataset:
    for func in func_dict.keys():
        if func not in functions:
            available_functions = ", ".join(functions.keys())
            raise ValueError(f"Function {func} not available. Choose from: {available_functions}.")

    funcs = []
    for func in func_dict.keys():
        real_func = functions[func]
        func_kwargs, _ = split_function_inputs(kwargs, real_func)
        funcs.append(partial(real_func, averaging_period=averaging_period, **func_kwargs))
    func_vars = list(func_dict.values())

    apply_func_kwargs = apply_func_kwargs or {"keep_attrs": True}

    if "drop_remaining_vars" not in apply_func_kwargs:
        apply_func_kwargs["drop_remaining_vars"] = False

    if "remainder_func" not in apply_func_kwargs:
        apply_func_kwargs["remainder_func"] = partial(mean_resample, averaging_period=averaging_period)

    result = apply_funcs(ds, funcs, func_vars, **apply_func_kwargs)

    if drop_na:
        result = result.dropna("time")

    return result


def make_default_resampler_dict(ds: xr.Dataset, species: str | None = None) -> dict[str, list[str]]:
    """Make dictionary mapping resampling functions to variables they will be applied to."""

    # build dict of resampling functions
    func_dict = {}
    data_vars = [str(dv) for dv in ds.data_vars]

    # see if we have number of observations, so we can do weighted resampling
    possible_n_obs = [dv for dv in data_vars if "number_of_observations" in dv]

    if possible_n_obs:
        # we need to know the species to do a weighted resample
        # try to guess species if not set
        if species is None:
            try:
                species = _guess_species(ds)
            except ValueError:
                pass

        if species is not None and species in data_vars:
            n_obs = possible_n_obs[0]
            data_vars.remove(n_obs)
            data_vars.remove(species)

            weighted_vars = [species, n_obs]

            if f"{species}_variability" in data_vars:
                weighted_vars.append(f"{species}_variability")
                data_vars.remove(f"{species}_variability")

            func_dict["weighted_resample"] = weighted_vars

    # now check for repeatability
    repeatability_vars = [dv for dv in data_vars if "repeatability" in dv]

    if repeatability_vars:
        func_dict["independent_uncertainties_resample"] = repeatability_vars

    return func_dict
