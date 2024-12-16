"""Combine multiple data objects (objects with .metadata and .data attributes) into one."""

from functools import partial
from typing import Any, cast, TypeVar
from collections.abc import Callable

import numpy as np
import xarray as xr
from xarray.core.types import CompatOptions

from openghg.types import HasMetadataAndData


T = TypeVar("T", bound=HasMetadataAndData)  # generic type for classes with .metadata and .data attributes


# TODO: review copying vs. mutating data. Is this handled nicely by xarray and dask?
# TODO: do we need to add options to use dask.delayed like xr.open_mfdataset?
def combine_data_objects(
    data_objects: list[T],
    preprocess: Callable | None = None,
    concat_dim: str | None = None,
    compat: CompatOptions | None = None,
    drop_duplicates: str | None = None,
) -> T:
    """Combine multiple data objects with optional preprocessing step.

    Args:
        data_objects: list of data objects (e.g. ObsData or FluxData, or any object with .metadata
            and .data attributes)
        preprocess: optional function to call on data objects; must accept type T and return type T
        concat_dim: optional dimension to specify concatenation along; if not specified, this is inferred via
            xr.combine_by_coords
        compat: 'compat' option to be passed to xr.concat or xr.combine_by_coords. See docs there.
        drop_duplicates: optional dimension to drop duplicates along, after combining.

    Returns:
        single data object with data from each object combined by coordinates, with optional preprocessing applied.
    """
    if not data_objects:
        # without knowing the type T, we can't return a sensible default
        raise ValueError("`data_objects` must be a non-empty list.")

    if preprocess:
        data_objects = [preprocess(do) for do in data_objects]

    # combine data by coordinates (this is what is used by xr.open_mfdataset by default; it combines and sorts)
    datasets = [do.data for do in data_objects]

    if concat_dim is not None:
        if compat is None:
            compat = "equals"
        new_data = cast(
            xr.Dataset, xr.concat(datasets, dim=concat_dim, combine_attrs="drop_conflicts", fill_value=np.nan)
        )
    else:
        if compat is None:
            compat = "no_conflicts"
        new_data = cast(
            xr.Dataset, xr.combine_by_coords(datasets, combine_attrs="drop_conflicts", fill_value=np.nan)
        )

    if drop_duplicates is not None:
        new_data = new_data.drop_duplicates(drop_duplicates)

    # combine metadata
    metadatas = [do.metadata for do in data_objects]
    new_metadata = metadatas[0].copy()  # TODO: add options for combining metadata

    # make a new data object of same type as input
    cls = type(data_objects[0])  # need non-empty list of data_objects to get this type
    result = cls(metadata=new_metadata, data=new_data)

    return result


# metadata combination methods


def _add_dim_from_metadata(
    data_object: T,
    new_dim: str,
    metadata_key: str | None = None,
    formatter: Callable = str,
    drop_metadata_key: bool = True,
) -> T:
    """Add a dimension `new_dim `to the data of T using the value corresponding
    to the key `new_dim` in the metadata of T.

    Args:
        data_object: object to modify
        new_dim: name of new dimension to add
        metadata_key: key of dimension value that will be retrieved from data_object.metadata.
            If None, then `new_dim` is used as the metadata key.
        formatter: function to apply to retrieved metadata value. Only applied if value is not None.
        drop_metadata_key: if True, del `metadata_key` from the metadata of the returned object.

    Returns:
        object of same type as `data_object` whose data has an extra dimension, and whose metadata is the
        same, possibly with the value of the new dimension removed.
    """
    if metadata_key is None:
        metadata_key = new_dim

    dim_val = data_object.metadata.get(metadata_key, None)
    if dim_val:
        dim_val = formatter(dim_val)

    new_data = data_object.data.expand_dims({new_dim: [dim_val]})

    new_metadata = data_object.metadata.copy()

    if drop_metadata_key and metadata_key in new_metadata:
        del new_metadata[metadata_key]

    # make a new data object of same type as input
    cls = type(data_object)
    result = cls(metadata=new_metadata, data=new_data)

    return result


def combine_multisite(data_objects: list[T]) -> T:
    """Combine data objects by adding a site dimension containing value of `site`
    stored in each object's metadata.
    """
    preprocess = partial(_add_dim_from_metadata, new_dim="site")
    return combine_data_objects(data_objects, preprocess=preprocess, concat_dim="site")


def _data_array_from_value(value: Any, coords: xr.Coordinates, name: str | None = None) -> xr.DataArray:
    """Create xr.DataArray with single value and given coords and name.

    Args:
        value: value to broadcast over coordinates (i.e. the constant value the DataArray should hold)
        coords: coordinates from an xr.Dataset or xr.DataArray
        name: name for output xr.DataArray

    Returns:
        xr.DataArray with single value for all coordinate values
    """
    shape = tuple(len(coord) for coord in coords.values())
    data = np.broadcast_to(value, shape)
    return xr.DataArray(data, coords=coords, name=name)


def add_variable_from_metadata(
    data_object: T,
    metadata_key: str,
    dims: str | list[str] = "time",
    formatter: Callable = str,
    drop_metadata_key: bool = True,
    new_metadata_value: Any = None,
    name: str | None = None,
) -> T:
    """Add a data variable to the data of data_object using the value corresponding
    to the key `metadata_key` in the metadata of data_object.

    Args:
        data_object: object to modify (the metadata is copied; the data is a view?)
        metadata_key: key of value that will be retrieved from data_object.metadata.
        dims: dimension or list of dimensions the new variable should have
        formatter: function to apply to retrieved metadata value. Only applied if value is not None.
        drop_metadata_key: if True, del `metadata_key` from the metadata of the returned object.
        new_metadata_value: value to save for metadata_key
        name: name for the new variable; if `None` then `metadata_key` will be used.

    Returns:
        object of same type as `data_object` whose data has an data variable with given coordinates and
         constant value, and whose metadata is the same, possibly with the value of the new dimension removed.
    """
    value = data_object.metadata.get(metadata_key, None)
    if value and formatter is not None:
        value = formatter(value)

    if isinstance(dims, str):
        dims = [dims]

    coords = xr.Coordinates({k: v for k, v in data_object.data.coords.items() if k in dims})
    if name is None:
        name = metadata_key
    new_da = _data_array_from_value(value, coords, name)

    new_data = xr.merge([data_object.data, new_da])

    new_metadata = data_object.metadata.copy()

    if drop_metadata_key:
        if metadata_key in new_metadata:
            del new_metadata[metadata_key]
        if metadata_key in new_data.attrs:
            del new_data.attrs[metadata_key]

    if new_metadata_value is not None:
        new_metadata[metadata_key] = new_metadata_value
        new_data.attrs[metadata_key] = new_metadata_value

    # make a new data object of same type as input
    cls = type(data_object)
    result = cls(metadata=new_metadata, data=new_data)

    return result


def combine_and_elevate_inlet(data_objects: list[T], override_on_conflict: bool = True) -> T:
    """Combine multiple data objects, elevating inlet from metadata to data variable with a time dimension.

    Args:
        data_objects: list of data objects to combine
        override_on_confict: if True, when the same time is present in multiple data objects,
            choose the value from the first data object where it occurs.

    Returns:
        data object of same type as input list, with added "inlet" data variable.
    """
    from openghg.util import extract_float

    def inlet_formatter(inlet: str) -> float:
        try:
            result = extract_float(inlet)
        except ValueError:
            return cast(float, np.nan)  # Mypy failing considering this as Any.
        else:
            return cast(float, result)

    preprocess = partial(
        add_variable_from_metadata,
        metadata_key="inlet",
        formatter=inlet_formatter,
        new_metadata_value="multiple",
    )

    if override_on_conflict:
        return combine_data_objects(
            data_objects, preprocess=preprocess, concat_dim="time", drop_duplicates="time"
        )
    return combine_data_objects(data_objects, preprocess=preprocess)
