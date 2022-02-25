""" Handles the recombination of dataframes stored in the object store
    into the data requested by the user

"""
from typing import Dict, List, Optional, Union
from xarray import Dataset, DataArray
from xarray.core.coordinates import DatasetCoordinates
import numpy as np
import xarray as xr

__all__ = ["recombine_multisite", "recombine_datasets"]


def recombine_multisite(keys: Dict, sort: Optional[bool] = True) -> Dict:
    """Recombine the keys from multiple sites into a single Dataset for each site

    Args:
        site_keys: A dictionary of lists of keys, keyed by site
        sort: Sort the resulting Dataset by the time dimension
    Returns:
        dict: Dictionary of xarray.Datasets
    """
    result = {}
    for key, key_list in keys.items():
        result[key] = recombine_datasets(keys=key_list, sort=sort)

    return result


def recombine_datasets(
    keys: List[str],
    sort: Optional[bool] = True,
    attrs_to_check: Dict[str, str] = None,
    elevate_inlet: bool = False,
) -> Dataset:
    """Combines datasets stored separately in the object store
    into a single dataset

    Args:
        keys: List of object store keys
        sort: Sort the resulting Dataset by the time dimension. Default = True
        attrs_to_check: Attributes to check for duplicates. If duplicates are present
            a new data variable will be created containing the values from each dataset
            If a dictionary is passed, the attribute(s) will be retained and the new value assigned.
            If a list/string is passed, the attribute(s) will be removed.
        elevate_inlet: Force the elevation of the inlet attribute
    Returns:
        xarray.Dataset: Combined Dataset
    """
    from xarray import concat as xr_concat
    from openghg.store.base import Datasource
    from openghg.objectstore import get_bucket

    if not keys:
        raise ValueError("No data keys passed.")

    bucket = get_bucket()

    data = [Datasource.load_dataset(bucket=bucket, key=k) for k in keys]

    # Check if we've got multiple inlet heights
    inlets_to_check = check_inlets(data=data, elevate_inlet=elevate_inlet)

    if attrs_to_check is None:
        attrs_to_check = {}

    attrs_to_check.update(inlets_to_check)

    # For specified attributes (e.g. "inlet")
    # elevate duplicates to data variables within each Dataset
    if attrs_to_check:
        # if isinstance(attrs_to_check, dict):
        attributes = list(attrs_to_check.keys())
        replace_values = list(attrs_to_check.values())

        # TODO - GJ - 2022-02-22 - I'm not sure we need to many different ways of passing in inlets to check here?
        # elif isinstance(attrs_to_check, str):
        #     attributes = [attrs_to_check]
        #     replace_values = [""]
        # else:
        #     attributes = attrs_to_check
        #     replace_values = [""] * len(attributes)

        data = elevate_duplicate_attrs(ds_list=data, attributes=attributes, elevate_inlet=elevate_inlet)

    # Concatenate datasets along time dimension
    combined = xr_concat(data, dim="time")

    # Replace/remove incorrect attributes
    #  - xr.concat will only take value from first dataset if duplicated
    if attrs_to_check:
        for attr, value in zip(attributes, replace_values):
            if attr in combined:  # Only update if attr was elevated to a data variable
                if value:
                    combined.attrs[attr] = value
                else:
                    combined.attrs.pop(attr)

    if sort:
        combined = combined.sortby("time")

    # This is modified from https://stackoverflow.com/a/51077784/1303032
    unique, index, count = np.unique(combined.time, return_counts=True, return_index=True)

    n_dupes = unique[count > 1].size
    if n_dupes > 5:
        raise ValueError("Large number of duplicate timestamps, check data overlap.")

    combined = combined.isel(time=index)

    return combined


def create_array_from_value(
    value: str,
    coords: Union[DatasetCoordinates, Dict[str, DatasetCoordinates]],  # type: ignore
    name: Union[str, None] = None,
) -> DataArray:
    """
    Create a new xarray.DataArray object containing a single value repeated
    for each coordinate.

    Args:
        value: Value to be repeated within the DataArray object
        coords: Co-ordinates to use for this new DataArray.
        name: Name to give the variable within the DataArray
    Returns:
        DataArray
    """
    if isinstance(coords, xr.core.coordinates.DatasetCoordinates):
        names = list(coords.keys())
        dims = tuple(len(coords[n]) for n in names)
    elif isinstance(coords, dict):
        dims = tuple(len(coord) for coord in list(coords.values()))
    else:
        dims = (len(coords),)

    variable = np.tile(value, dims)
    data_variable = xr.DataArray(variable, coords=coords, name=name)

    return data_variable


def elevate_duplicate_attrs(
    ds_list: List[Dataset], attributes: Union[str, List[str]], elevate_inlet: bool
) -> List[Dataset]:
    """
    For a list of Datasets, if the specified attributes are being repeated
    these will be added as new data variables to each Dataset.

    Args:
        ds_list: List of xarray Datasets
        attributes: Attribute values to check within the Datasets. If None is passed
        the original dataset list will be returned.
        elevate_inlet: Force the elevation of inlet
    Returns:
        list: List of updated Dataset objects
    """
    if not isinstance(attributes, list):
        attributes = [attributes]

    for attr in attributes:
        # Pull the attributes out of the datasets - usually inlet values for ranked data
        data_attrs = [ds.attrs[attr] for ds in ds_list if attr in ds.attrs]

        # If we have more than one unique value we update the Dataset by adding a new variable
        # This is useful with ranked inlets so we can easily know which inlet a measurement was taken from
        if len(set(data_attrs)) > 1 or (attr == "inlet" and elevate_inlet):
            for i, ds in enumerate(ds_list):
                value = ds.attrs[attr]
                coords = ds.coords
                new_variable = create_array_from_value(value=value, coords=coords, name=attr)
                updated_ds = ds.assign({attr: new_variable})

                ds_list[i] = updated_ds

    return ds_list


def check_inlets(data: List[Dataset], elevate_inlet: bool) -> Dict:
    """Check the inlets of the data to be processed

    Args:
        data: List of Datasets
    Returns:
        dict: Dictionary with single or multiple inlet replacement value
    """
    inlets = set()

    for dataset in data:
        try:
            inlets.add(dataset.attrs["inlet"])
        except KeyError:
            pass

    if len(inlets) > 1:
        attrs = {"inlet": "multiple"}
    else:
        if elevate_inlet:
            attrs = {"inlet": inlets.pop()}
        else:
            attrs = {}

    return attrs
