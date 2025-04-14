"""Functions for resampling.

Functions for different types of resampling are registered
by name in the `registry`.

By convention, the names of these functions end in `_resample`,
and the function name, without `_resample` is how the function
is listed in the registry.

These functions include:
- `mean_resample` (registered as "mean")
- `weighted_resample` (registered as "weighted")
- `variability_resample` (registered as "variability")
- `uncorrelated_errors_resample` (registered as "uncorrelated_errors")

The `resampler` function takes a dictionary mapping the registered names
(e.g. "mean", "weighted", etc.) to lists of variables those methods should
be applied to.

The standard options for resampling surface obs. data (in `get_obs_surface`)
are applied by the function `surface_obs_resampler`.

If custom resampling is needed, the user can write their own resampling function,
possible using `surface_obs_resampler` as a base.
"""

from collections import defaultdict
from collections.abc import Callable, Sequence
from functools import partial, wraps
from typing import Any, Concatenate, Literal

import logging
import pandas as pd
import xarray as xr
from openghg.util import Registry
from typing_extensions import ParamSpec

from ._attrs import rename, update_attrs
from ._xarray_helpers import xr_sqrt


registry = Registry(suffix="resample")
register = registry.register

logger = logging.getLogger("openghg.data_processing")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


# somewhat complicated typing for decorator:
# we need to use `ParamSpec` to represent *args and **kwargs
P = ParamSpec("P")

# resampling functions take: a dataset, an averaging period (str), and possibly *args and **kwargs
ResampleFunctionType = Callable[Concatenate[xr.Dataset, str, P], xr.Dataset]


def add_averaging_attrs(func: ResampleFunctionType) -> ResampleFunctionType:
    """Decorator to add averaging attributes to result of resampling function."""

    @wraps(func)
    def wrapper(ds: xr.Dataset, averaging_period: str, *args: P.args, **kwargs: P.kwargs) -> xr.Dataset:
        average_in_seconds = pd.Timedelta(averaging_period).total_seconds()
        avg_attrs = {"averaged_period": average_in_seconds, "averaged_period_str": averaging_period}
        return func(ds, averaging_period, *args, **kwargs).assign_attrs(avg_attrs)

    return wrapper


@register
@add_averaging_attrs
def mean_resample(ds: xr.Dataset, averaging_period: str) -> xr.Dataset:
    """Resample to mean over averaging period.

    Args:
        ds: xr.Dataset to resample
        averaging_period: period to resample to; should be a valid pandas "offset alias"

    Returns:
        xr.Dataset with all data variables mean resampled over averaging period
    """
    ds_resampled = ds.resample(time=averaging_period).mean(skipna=False, keep_attrs=True)

    return ds_resampled


@register
@add_averaging_attrs
def weighted_resample(ds: xr.Dataset, averaging_period: str, species: str) -> xr.Dataset:
    """Resample concentration and variability, weighted by number of observations.

    Successive applications of this method are consistent with a single equivalent application.
    For instance, resampling to 1h then to 4h, will give the same result as resampling to 4h
    in one step.

    Args:
        ds: xr.Dataset to resample
        averaging_period: period to resample to; should be a valid pandas "offset alias"
        species: species data applies to; a data variable with this name, as well as
            a data variable named {species}_number_of_observations must be present in `ds`.

    Returns:
        xr.Dataset with obs. (and variability) resampled, weighted by the number of obs.

    Raises:
        ValueError: if obs. or number of obs. is not present in the dataset.
    """
    n_obs = f"{species}_number_of_observations"

    if n_obs not in ds or species not in ds:
        raise ValueError(
            f"Cannot do weighted resample without variables `{species}` and `{species}_number_of_observations`."
        )

    mf_variability = None
    if f"{species}_variability" in ds:
        mf_variability = ds[f"{species}_variability"]

    result = _weighted_resample(
        mf=ds[species],
        n_obs=ds[n_obs],
        averaging_period=averaging_period,
        species=species,
        mf_variability=mf_variability,
    ).assign_attrs(ds.attrs)

    return result


def _weighted_resample(
    mf: xr.DataArray,
    n_obs: xr.DataArray,
    averaging_period: str,
    mf_variability: xr.DataArray | None = None,
    species: str = "mf",
) -> xr.Dataset:
    """Resample concentration (and variability), weighting by number of observations.

    Resampling to a frequency, say 4h, will give the same result, even if the data is first resampled
    to an intermediate frequency.

    For example:

    >>> ds_1h = _weighted_resample(mf, n_obs, "1h", mf_variability)
    >>> ds_4h = _weighted_resample(ds_1h.mf, ds_1h.mf_number_of_observations, "4h", ds_1h.mf_variability)
    >>> ds_4h_2 = _weighted_resample(mf, n_obs, "4h", mf_variability)
    >>> xr.testing.assert_all_close(ds_4h, ds_4h_2)

    Note: to ensure this consistency, you must drop NaN values, or pass kwargs to `sum` to ensure that
    a resampling period containing only NaNs is not resampled to 0.

    For instance: `sum_kwargs = {"skipna": True, "min_count": 1}`

    See https://github.com/pydata/xarray/issues/4291 for more discussion.

    Args:
        mf: observations to resample by taking weighted mean
        n_obs: number of observations; will be resampled by summing
        averaging_period: period to resample to; should be a valid pandas "offset alias"
        mf_variability: optional "variability" to resample
        species: species the obs. apply to; this is used to name the output variables.

    Returns:
        xr.Dataset: with obs., number of obs., (and variability) resampled
    """
    sum_kwargs: dict[str, Any] = {"skipna": True, "min_count": 1, "keep_attrs": True}

    with xr.set_options(keep_attrs=True):
        n_obs_resample_sum = n_obs.resample(time=averaging_period).sum(**sum_kwargs)

        weighted_resample_mf = (mf * n_obs).resample(time=averaging_period).sum(
            **sum_kwargs
        ) / n_obs_resample_sum

        data_vars = {species: weighted_resample_mf, f"{species}_number_of_observations": n_obs_resample_sum}

        if mf_variability is not None:
            sums_of_squares = n_obs * (mf_variability**2 + mf**2)

            weighted_resample_mf_variability_squared = (
                sums_of_squares.resample(time=averaging_period).sum(**sum_kwargs) / n_obs_resample_sum
                - weighted_resample_mf**2
            )
            weighted_resample_mf_variability = xr_sqrt(weighted_resample_mf_variability_squared)

            data_vars[f"{species}_variability"] = weighted_resample_mf_variability

    return xr.Dataset(data_vars=data_vars)


@register
@add_averaging_attrs
def mean_and_variability_resample(ds: xr.Dataset, averaging_period: str, species: str) -> xr.Dataset:
    """Resample concentration and variability, using weighted_resample and assuming number of observations uniformely equal to 1.

    Args:
        ds: xr.Dataset to resample
        averaging_period: period to resample to; should be a valid pandas "offset alias"
        species: species data applies to; a data variable with this name, as well as
            a data variable named {species}_variability must be present in `ds`.

    Returns:
        xr.Dataset with obs. (and variability) resampled.
    """

    n_obs = f"{species}_number_of_observations"
    ds[n_obs] = xr.full_like(ds[species], fill_value=1)
    ds[n_obs].attrs = {"long_name": "faked number of observations for weighted_resample function"}

    result = weighted_resample(ds, averaging_period, species)

    del result[n_obs]

    return result


@register
@add_averaging_attrs
def uncorrelated_errors_resample(
    ds: xr.Dataset,
    averaging_period: str,
    sum_kwargs: dict | None = None,
) -> xr.Dataset:
    """Resample uncertainties as the standard deviations of an average of independent quantities.

    We assume that each uncertainty value is the standard deviation of one of the quantities
    being averaged.

    That is, we use the formula

    Var((X_1 + ... + X_n) / n) = (Var(X_1) + ... + Var(X_n)) / n^2

    on each averaging period, where Var(X_i) = (uncertainty of observation X_i)^2.

    Args:
        ds: xr.Dataset to resample
        averaging_period: period to resample to; should be a valid pandas "offset alias"
        sum_kwargs: arguments to pass to xr.Dataset.sum

    Returns:
        xr.Dataset with all data variables mean resampled over averaging period
    """
    sum_kwargs = sum_kwargs or {}

    with xr.set_options(keep_attrs=True):
        n_obs = ds.resample(time=averaging_period).count()
        data_resampled_squared = (ds**2).resample(time=averaging_period).sum(**sum_kwargs) / n_obs**2

        result = xr_sqrt(data_resampled_squared)

    return result


@register
@add_averaging_attrs
def variability_resample(ds: xr.Dataset, averaging_period: str, fill_zero: bool = True) -> xr.Dataset:
    """Compute variability as stdev of observed mole fraction over averaging periods.

    Args:
        ds: xr.Dataset to resample
        averaging_period: period to resample to; should be a valid pandas "offset alias"
        fill_zero: if True, fill zeros with median. (If there is only one value in a resampling
            period, the stdev is zero.)

    Returns:
        xr.Dataset with all data variables resampled to standard deviation over averaging period
    """
    result = ds.resample(time=averaging_period).std(keep_attrs=True)

    result = rename(result, lambda x: x + "_variability")

    if fill_zero:
        # we can't filter by a dask array, so we need to call compute
        result = result.compute()
        result = result.where(result == 0.0, result.median(dim="time"))

    result = update_attrs(result, (lambda x: x + "_variability", ["long_name"]))

    return result


# typing for `apply_funcs`
DatasetOpType = Callable[Concatenate[xr.Dataset, P], xr.Dataset]


def apply_funcs(
    ds: xr.Dataset,
    funcs: Sequence[DatasetOpType],
    func_vars: Sequence[list[str]],
    remainder: Callable | Literal["pass", "drop"] = "pass",
    keep_attrs: bool | Literal["default"] = True,
) -> xr.Dataset:
    """Apply functions to the data variables of a dataset.

    For each function, there is a corresponding list of variables that the function will be
    applied to.

    Variables not in any list in `func_vars` is a "remaining" variable. These can be dropped
    or passed through unchanged; alternatively, `remainder_func` can be applied to all remaining
    variables.

    Note: it is possible for a data variables to appear in more than one list in `func_vars`; this
    variable will have multiple functions applied to it. In this case, the function should rename
    the variables returned to avoid name conflicts.

    Args:
        ds: dataset to apply functions to
        funcs: list of functions to apply to certain data variables of dataset
        func_vars: list of lists of data variables; the first function in `funcs` will be applied to
            the first list of data variables in `func_vars`, and so on.
        remainder: species how to handle data variables that were not modified by any function in `funcs`.
            There are three options:
            1. if a function is passed, this is applied to all remaining variables.
            2. if "pass", the remaining variables are passed through unchanged.
            3. if "drop", the remaining variables are dropped.
        keep_attrs: if True, make global attributes of output dataset the same as the global attributes
            of input dataset.

    Returns:
        dataset with functions applied to specified variables.

    Raises:
        ValueError: if length of `funcs` and `func_vars` is not equal.
    """
    if len(funcs) != len(func_vars):
        raise ValueError(
            "Length of `funcs` and `func_vars` must be equal;"
            f"got lists of length {len(funcs)} and {len(func_vars)}."
        )

    with xr.set_options(keep_attrs=keep_attrs):
        results = []

        for func, fvars in zip(funcs, func_vars):
            results.append(func(ds[fvars]))

        if remainder != "drop":
            all_func_vars = [x for y in func_vars for x in y]
            remaining_vars = [str(dv) for dv in ds.data_vars if dv not in all_func_vars]

            if remainder == "pass":
                results.append(ds[remaining_vars])
            else:
                results.append(ds[remaining_vars].map(remainder))

        return xr.merge(results)


@add_averaging_attrs
def resampler(
    ds: xr.Dataset,
    averaging_period: str,
    func_dict: dict[str, list[str]],
    drop_na: bool = True,
    apply_func_kwargs: dict | None = None,
    **kwargs: Any,
) -> xr.Dataset:
    """Resample data variables in dataset using functions specified in func_dict.

    Use `registry.describe()` to print a list of valid resampling functions
    to use in `func_dict`.

    For example,
    ```
    func_dict = {
                 "mean": ["ch4"],
                 "uncorrelated_errors": ["ch4_repeatability"],
                 "stdev": ["ch4"],
    }
    ```
    would resample the variable "ch4" by taking the mean, the variable "ch4_repeatability" using
    "uncorrelated_errors_resample" (the default for "repeatability" variables), and would add
    variability by taking the standard deviation of the obs. over the resampling period.


    Args:
        ds: dataset to resample
        averaging_period: period to resample over.
        func_dict: dictionary mapping function names to data variables.
        drop_na: if True, drop NaNs along time axis.
        apply_func_kwargs: optional dict of arguments to pass to `apply_funcs`
        **kwargs: options that will be passed to the resampling functions. If
            If the key of an option has the form "<function name>__<key>", then
            the option for <key> will be applied only to the function with name
            <function name>.

    Returns:
        resampled xr.Dataset
    """
    kwargs["averaging_period"] = averaging_period

    # retrieve functions from registry and apply arguments
    funcs = []
    for func_name in func_dict:
        func = registry.functions[func_name]
        func_kwargs = registry.select_params(func_name, kwargs)
        funcs.append(partial(func, **func_kwargs))

    func_vars = list(func_dict.values())

    apply_func_kwargs = apply_func_kwargs or {"keep_attrs": True}

    if "remainder" not in apply_func_kwargs:
        apply_func_kwargs["remainder"] = partial(mean_resample, averaging_period=averaging_period)

    result = apply_funcs(ds, funcs, func_vars, **apply_func_kwargs)

    if drop_na:
        result = result.dropna("time")

    return result


def _surface_obs_resampler_dict(ds: xr.Dataset, species: str) -> dict[str, list[str]]:
    """Make dictionary mapping resampling functions to variables they will be applied to.

    This is a helper function used by `surface_obs_resampler`.

    Args:
        ds: surface obs. data that resampler will be applied to
        species: species of the obs. data

    Returns:
        dict mapping resampler function names to lists of data variables.
    """
    # build dict of resampling functions
    func_dict = defaultdict(list)
    data_vars = [str(dv) for dv in ds.data_vars]

    # check for repeatability
    repeatability = f"{species}_repeatability"

    if repeatability in data_vars:
        func_dict["uncorrelated_errors"] = [repeatability]

    # if we have "number of observations", do weighted resampling
    variability = f"{species}_variability"
    variability_set = False

    n_obs = f"{species}_number_of_observations"

    if species in data_vars and n_obs not in data_vars and variability in data_vars:
        func_dict["mean_and_variability"] = [species, variability]

        variability_set = True

    elif n_obs in data_vars and species in data_vars:
        weighted_vars = [species, n_obs]

        if variability in data_vars:
            weighted_vars.append(variability)

        func_dict["weighted"] = weighted_vars

        variability_set = True

    # if we didn't do a weighted resample for variability, report the stdev of the mole fraction
    if not variability_set and species in data_vars:
        func_dict["variability"] = [species]

        # since species is mapped to species_variability, it will not be mean resampled by `resampler` by
        # default, so set this explicitly
        if species not in func_dict.get("weighted", []):
            func_dict["mean"].append(species)

    return func_dict


def surface_obs_resampler(
    ds: xr.Dataset,
    averaging_period: str,
    species: str,
    drop_na: bool = True,
) -> xr.Dataset:
    """Apply default resampling options for surface obs. data.

    If the data contains the number of observations as a data variable, the
    this data variable and the species mole fraction will be resampling using
    a weighted average. Additionally, if "variability" is present, it will be
    resampled using weights as well.

    Otherwise, the species mole fraction is resampled to the mean.

    If "repeatability" is present, it is resampled using the "uncorrelated_errors" method.

    If "variability" is not present, it is added by taking the standard deviation of the obs.

    Any remaining variables are mean resampled.

    Keeps attributes from original dataset.

    Args:
        ds: surface obs. data that resampler will be applied to
        averaging_period: period to resample to; should be a valid pandas "offset alias"
        species: species of the obs. data
        drop_na: if True, drop NaNs along "time" dimension.

    Returns:
        xr.Dataset resampled according to default specification.
    """
    resampler_dict = _surface_obs_resampler_dict(ds, species)

    result = resampler(ds, averaging_period, resampler_dict, species=species, drop_na=False)

    if drop_na:
        check_any = [str(dv) for dv in ds.data_vars if str(dv) in [species, "inlet"]]
        check_all = [
            str(dv)
            for dv in ds.data_vars
            if str(dv) in [f"{species}_variability", f"{species}_repeatability"]
        ]
        result = result.dropna("time", subset=check_any, how="any")
        result = result.dropna("time", subset=check_all, how="all")

    return result
