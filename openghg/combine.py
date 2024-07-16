"""
Combine multiple data objects (subclasses of _BaseData) into one.
"""
from typing import Callable, cast, Optional, TypeVar

import xarray as xr

from openghg.types import HasMetadataAndData


T = TypeVar("T", bound=HasMetadataAndData)  # generic type for classes with .metadata and .data attributes


# TODO: make metdata combination methods


def combine_data_objects(data_objects: list[T], preprocess: Optional[Callable] = None) -> T:
    """Combine multiple data objects with optional preprocessing step.

    Args:
        data_objects: list of data objects (subclasses of _BaseData, e.g. ObsData or FluxData)
        preprocess: optional function to call on data objects; must accept type T and return type T

    Returns:
        single data object with data from each object combined by coordinates, with optional preprocessing applied.
    """
    if not data_objects:
        # without knowing the type T, we can't return a sensible default
        raise ValueError("`data_objects` must be a non-empty list.")

    if preprocess:
        data_objects = [preprocess(do) for do in data_objects]

    datasets = [do.data for do in data_objects]
    metadatas = [do.metadata for do in data_objects]

    # combine data by coordinates (this is what is used by xr.open_mfdataset by default; it combines and sorts)
    new_data = cast(xr.Dataset, xr.combine_by_coords(datasets))  # mypy thinks this is an xr.DataArray instead of xr.Dataset

    # combine metadata
    new_metadata = metadatas[0]  # TODO: add options for combining metadata

    # make a new data object of same type as input
    cls = type(data_objects[0])  # need non-empty list of data_objects to get this type
    result = cls(metadata=new_metadata, data=new_data)

    return result
