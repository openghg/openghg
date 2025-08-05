from collections.abc import Iterable, Sequence
from typing import Any, cast

import numpy as np
import xarray as xr
from xarray import AlignmentError

from openghg.types import ReindexMethod, XrDataLikeMatch


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


def reindex_on_dims(
    to_reindex: XrDataLikeMatch,
    reindex_like: xr.DataArray | xr.Dataset,
    dims: str | Sequence[str],
    method: ReindexMethod = "nearest",
    tolerance: float | Iterable[float] = 1e-5,
) -> XrDataLikeMatch:
    """Reindex along selected dimensions.

    Reindex DataArray or Dataset 'to_reindex' like the DataArray or Dataset
    'reindex_like', but only along the specified dimensions.

    Args:
        to_reindex: DataArray or Dataset to reindex.
        reindex_like: DataArray or Dataset to reindex like.
        dims: dimension(s) to reindex along.
        method: `None`, 'nearest', 'pad'/'ffill', 'backfill'/'bfill' - method
          for filling index values in 'reindex_like' not found in 'to_reindex'.
        tolerance: maximum distance between original and new labels for inexact
          matches. If list-like, must has the same length as `dims`.

    Returns:
        `to_reindex` reindexed along selected dims.

    """
    if isinstance(dims, str):
        dims = [dims]

    indexers = {dim: reindex_like[dim] for dim in dims}

    try:
        result = to_reindex.reindex(indexers, method=method, tolerance=tolerance)
    except AlignmentError as e1:
        try:
            result = to_reindex.pint.reindex(indexers, method=method, tolerance=tolerance)
        except ValueError as e2:
            raise ValueError(
                f"Could not reindex on dims due to error with xr.Dataset.reindex:\n{e1}\nand error with pint.reindex:\n{e2}."
            ) from e2

    # pint.reindex doesn't have correct type hints?
    return result  # type: ignore


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

    # align to highest frequency dataset
    datasets = list(datasets)
    data_highest_freq = datasets.pop(index_highest_freq)
    coord_to_match = data_highest_freq[dim]
    datasets = [ds.reindex({dim: coord_to_match}, method=method) for ds in datasets]
    datasets.append(data_highest_freq)

    # quantify and sum
    result = cast(xr.Dataset, sum(ds.pint.quantify() for ds in datasets))

    return cast(xr.Dataset, result.pint.dequantify())
