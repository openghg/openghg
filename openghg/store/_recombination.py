""" Handles the recombination of dataframes stored in the object store
    into the data requested by the user

"""
from typing import Dict, List, Optional
from xarray import Dataset

__all__ = ["recombine_multisite", "recombine_datasets"]


def recombine_multisite(keys: Dict, sort: Optional[bool] = True) -> Dict:
    """ Recombine the keys from multiple sites into a single Dataset for each site

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


def recombine_datasets(keys: List[str], sort: Optional[bool] = True) -> Dataset:
    """Combines datasets stored separately in the object store
    into a single datasets

    Args:
        keys: List of object store keys
        term
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

    combined = xr_concat(data, dim="time")

    if sort:
        combined = combined.sortby("time")

    # Check for duplicates?
    # This is taken from https://stackoverflow.com/questions/51058379/drop-duplicate-times-in-xarray
    # _, index = np.unique(combined['time'], return_index=True)
    # combined = combined.isel(time=index)

    return combined
