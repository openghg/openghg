from collections.abc import Sequence
from typing import Any, cast

import numpy as np
import xarray as xr

from openghg.types import ReindexMethod


def _indexes_match(dataset_A: xr.Dataset, dataset_B: xr.Dataset) -> bool:
    """Check if two datasets need to be reindexed_like for combine_datasets

    Args:
        dataset_A: First dataset to check
        dataset_B: Second dataset to check
    Returns:
        bool: True if indexes match, else False
    """
    common_indices = (key for key in dataset_A.indexes.keys() if key in dataset_B.indexes.keys())

    for index in common_indices:
        if not len(dataset_A.indexes[index]) == len(dataset_B.indexes[index]):
            return False

        # Check number of values that are not close (testing for equality with floating point)
        if index == "time":
            # For time override the default to have ~ second precision
            rtol = 1e-10
        else:
            rtol = 1e-5

        index_diff = np.sum(
            ~np.isclose(
                dataset_A.indexes[index].values.astype(float),
                dataset_B.indexes[index].values.astype(float),
                rtol=rtol,
            )
        )

        if not index_diff == 0:
            return False

    return True


def combine_datasets(
    dataset_A: xr.Dataset,
    dataset_B: xr.Dataset,
    method: ReindexMethod = "ffill",
    tolerance: float | None = None,
) -> xr.Dataset:
    """Merges two datasets and re-indexes to the first dataset.

    If "fp" variable is found within the combined dataset,
    the "time" values where the "lat", "lon" dimensions didn't match are removed.

    Args:
        dataset_A: First dataset to merge
        dataset_B: Second dataset to merge
        method: One of None, nearest, ffill, bfill.
                See xarray.DataArray.reindex_like for list of options and meaning.
                Defaults to ffill (forward fill)
        tolerance: Maximum allowed tolerance between matches.

    Returns:
        xarray.Dataset: Combined dataset indexed to dataset_A
    """
    if _indexes_match(dataset_A, dataset_B):
        dataset_B_temp = dataset_B
    else:
        # load dataset_B to avoid performance issue (see xarray issue #8945)
        dataset_B_temp = dataset_B.load().reindex_like(dataset_A, method=method, tolerance=tolerance)

    merged_ds = dataset_A.merge(dataset_B_temp)

    if "fp" in merged_ds:
        if all(k in merged_ds.fp.dims for k in ("lat", "lon")):
            flag = np.isfinite(merged_ds.fp.mean(["lat", "lon"]))
            merged_ds = merged_ds.where(flag.compute(), drop=True)

    return merged_ds


def match_dataset_dims(
    datasets: Sequence[xr.Dataset],
    dims: str | Sequence = [],
    method: ReindexMethod = "nearest",
    tolerance: float | dict[str, float] = 1e-5,
) -> list[xr.Dataset]:
    """Aligns datasets to the selected dimensions within a tolerance.
    All datasets will be aligned to the first dataset within the list.

    Args:
        datasets: List of xarray Datasets. Expect datasets to contain the same dimensions.
        dims: Dimensions match between datasets. Can use keyword "all" to match every dimension.
        method : Method to use for indexing. Should be one of: ("nearest", "ffill", "bfill")
        tolerance: Tolerance value to use when matching coordinate values.
                   This can be a single value for all dimensions or a dictionary of values to use.

    Returns:
        List (xarray.Dataset) : Datasets aligned along the matching dimensions.

    TODO: Check if this supercedes or replicates _indexes_match() function too closely?
    """
    # Nothing to be done if only one (or less) datasets are passed
    if len(datasets) <= 1:
        return list(datasets)

    ds0 = datasets[0]

    if isinstance(dims, str):
        if dims == "all":
            dims = list(ds0.dims)
        else:
            dims = [dims]

    # Extract coordinate values for the first dataset in the list
    ds0 = datasets[0]
    indexers = {dim: ds0[dim] for dim in dims}

    if isinstance(tolerance, float):
        tolerance = {dim: tolerance for dim in dims}

    # Align datasets along selected dimensions (if not already identical)
    datasets_aligned = [ds0]
    for ds in datasets[1:]:
        for dim, compare_coord in indexers.items():
            try:
                coord = ds[dim]
            except KeyError:
                raise ValueError(f"Dataset missing dimension: {dim}")
            else:
                if not coord.equals(compare_coord):
                    ds = ds.reindex({dim: compare_coord}, method=method, tolerance=tolerance[dim])

        datasets_aligned.append(ds)

    return datasets_aligned


# TODO: calc_dim_resolution isn't used anywhere. But it is probably useful. Also maybe it should be in the top-level util.
def calc_dim_resolution(dataset: xr.Dataset, dim: str = "time") -> Any:
    """Calculates the average frequency along a given dimension.

    Args:
        dataset : Dataset. Must contain the specified dimension
        dim : Dimension name

    Returns:
        np.timedelta64 / np.float / np.int : Resolution with input dtype

        NaT : If unsuccessful and input dtype is np.timedelta64
        NaN : If unsuccessful for all other dtypes.
    """
    try:
        return _calc_time_dim_resolution(dataset, time_dim=dim)
    except ValueError:
        return _calc_average_gap(dataset[dim])


def _calc_time_dim_resolution(dataset: xr.Dataset, time_dim: str = "time") -> np.timedelta64:
    """Calculate average frequency of time dimension.

    Args:
        dataset: xr.Dataset with time coordinate
        time_dim: name of the time coordinate

    Returns:
        timedelta representing average gap between time points, or NaT if only
        one time is present

    Raises:
        ValueError: if the data type of `dataset[time_dim]` is not a subtype of
        `np.datetime64`. (Note: `np.timedelta64` is a separate type, and not
         a subtype of `np.datetime64`; differences between `np.datetime64` values
        are `np.timedelta64`.)

    """
    if not np.issubdtype(dataset[time_dim].dtype, np.datetime64):
        raise ValueError(
            f"Type {dataset[time_dim].dtype} of values in {time_dim} coordinate is not a subtype of `np.datetime64`."
        )

    try:
        resolution = dataset[time_dim].diff(dim=time_dim).mean().values
    except ValueError:
        return np.timedelta64("NaT")
    else:
        # cast because we already checked that the values in dataset[time_dim] are compatible with `np.datetime64`
        # so their diffs will be time deltas
        return cast(np.timedelta64, resolution)


def _calc_average_gap(data_array: xr.DataArray) -> Any:
    """Calculate average gap in DataArray.

    No checking is performed to guarantee a return type.

    Args:
        data_array: xr.DataArray whose values should support arithmetic, and should be 1D.

    Returns:
        average gap between values in DataArray.

    """
    if data_array.ndim > 1:
        raise ValueError("Input DataArray has more than 1 dimension.")

    dim = data_array.dims[0]

    try:
        return data_array.diff(dim=dim).mean().values
    except TypeError as e:
        # UFuncTypeError from not being able to take diff
        raise ValueError("Data in given DataArray does not support subtraction.") from e
    except ValueError as e:
        # check if coordinate has length 1, and return np.nan if so
        if data_array[dim].size == 1:
            return np.nan
        raise e  # else, re-raise


def stack_datasets(
    datasets: Sequence[xr.Dataset], dim: str = "time", method: ReindexMethod = "ffill"
) -> xr.Dataset:
    """Stacks multiple datasets based on the input dimension. By default this is time
    and this will be aligned to the highest resolution / frequency
    (smallest difference betweeen coordinate values).

    At the moment, the two datasets must have identical coordinate values for all
    other dimensions and variable names for these to be stacked.

    Args:
        datasets : Sequence of input datasets
        dim : Name of dimension to stack along. Default = "time"
        method: Method to use when aligning the datasets. Default = "ffill"

    Returns:
        Dataset : Stacked dataset

    TODO: Could update this to only allow DataArrays to be included to reduce the phase
    space here.
    """
    if len(datasets) == 1:
        dataset = datasets[0]
        return dataset

    data_frequency = [calc_dim_resolution(ds, dim) for ds in datasets]
    index_highest_freq = min(range(len(data_frequency)), key=data_frequency.__getitem__)
    data_highest_freq = datasets[index_highest_freq]
    coords_to_match = data_highest_freq[dim]

    for i, data in enumerate(datasets):
        data_match = data.reindex({dim: coords_to_match}, method=method)
        if i == 0:
            data_stacked = data_match
            data_stacked.attrs = {}
        else:
            data_stacked += data_match

    return data_stacked


def check_units(data_var: xr.DataArray, default: float) -> float:
    """Check "units" attribute within a DataArray. Expect this to be a float
    or possible to convert to a float.
    If not present, use default value.
    """
    attrs = data_var.attrs
    if "units" in attrs:
        units_from_attrs = attrs["units"]
        if not isinstance(units_from_attrs, float):
            try:
                units = float(units_from_attrs)
            except ValueError:
                raise ValueError(f"Cannot extract {units_from_attrs} value as float")
    else:
        units = default

    return units
