""" Handles the recombination of dataframes stored in the object store
    into the data requested by the user

"""
from typing import Dict, List, Optional, Union
from xarray import Dataset, DataArray
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


def recombine_datasets(keys: List[str],
                       sort: Optional[bool] = True,
                       attrs_to_check: Union[str, List[str], Dict[str, str], None] = {"inlet": "multiple"}) -> Dataset:
    """Combines datasets stored separately in the object store
    into a single datasets

    Args:
        keys: List of object store keys
        sort: Sort the resulting Dataset by the time dimension. Default = True
        attrs_to_check: Attributes to check for duplicates. If duplicates are present
            a new data variable will be created containing the values from each dataset
            If a dictionary is passed, the attribute(s) will be retained and the new value assigned.
            If a list/string is passed, the attribute(s) will be removed.

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

    # For specified attributes (e.g. "inlet")
    # elevate duplicates to data variables within each Dataset
    if attrs_to_check:
        if isinstance(attrs_to_check, dict):
            attributes = attrs_to_check.keys()
            replace_values = attrs_to_check.values()
        elif isinstance(attrs_to_check, str):
            attributes = [attrs_to_check]
            replace_values = [None]
        else:
            attributes = attrs_to_check
            replace_values = [None]*len(attributes)

        data = elevate_duplicate_attrs(data, attributes)

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

    # Check for duplicates?
    # This is taken from https://stackoverflow.com/questions/51058379/drop-duplicate-times-in-xarray
    # _, index = np.unique(combined['time'], return_index=True)
    # combined = combined.isel(time=index)

    return combined


def create_array_from_value(value: str,
                            coords: Union[DataArray, Dict],
                            name: Union[str, None] = None) -> DataArray:
    '''
    Create a new xarray.DataArray object containing a single value repeated
    for each coordinate.

    Args:
        value : Value to be repeated within the DataArray object
        coords : Co-ordinates to use for this new DataArray.
        name : Name to give the variable within the DataArray

    Returns:
        DataArray
    '''

    if isinstance(coords, xr.core.coordinates.DatasetCoordinates):
        names = list(coords.keys())
        dims = tuple(len(coords[n]) for n in names)
    elif isinstance(coords, dict):
        dims = (len(coord) for coord in list(coords.values()))
    else:
        dims = (len(coords), )

    variable = np.tile(value, dims)
    data_variable = xr.DataArray(variable, coords=coords, name=name)

    return data_variable


def elevate_duplicate_attrs(ds_list: List[Dataset], attrs: Union[str, List[str]]) -> List[Dataset]:
    '''
    For a list of Datasets, if the specified attributes are being repeated
    these will be added as new data variables to each Dataset.

    Args:
        ds_list : List of xarray Datasets
        attrs : Attribute values to check within the Datasets. If None is passed
            the original dataset list will be returned.

    Returns:
        List[Dataset] : Modified list of Dataset objects
    '''

    if isinstance(attrs, str):
        attributes = [attrs]
    else:
        attributes = attrs

    for attr in attributes:
        data_attr = [ds.attrs[attr] for ds in ds_list if attr in ds.attrs]
        if len(set(data_attr)) > 1:
            for i, ds in enumerate(ds_list):
                value = ds.attrs[attr]
                coords = ds.coords
                new_variable = create_array_from_value(value, coords, name=attr)
                ds_list[i] = ds.assign({attr: new_variable})

    return ds_list
