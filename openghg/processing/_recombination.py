""" Handles the recombination of dataframes stored in the object store
    into the data requested by the user

"""
from typing import List
from xarray import Dataset

__all__ = ["recombine_sections"]


def recombine_sections(data_keys: List[str], sort=True) -> Dataset:
    """ Combines datasets stored separately in the object store
        into a single datasets

        Args:
            data_keys: List of object store keys
            term
        Returns:
            xarray.Dataset: Combined Dataset
    """
    from xarray import concat as xr_concat
    from openghg.modules import Datasource
    from openghg.objectstore import get_bucket

    bucket = get_bucket()

    data = [Datasource.load_dataset(bucket=bucket, key=k) for k in data_keys]

    combined = xr_concat(data, dim="time")

    if sort:
        combined = combined.sortby("time")

    # Check for duplicates?
    # This is taken from https://stackoverflow.com/questions/51058379/drop-duplicate-times-in-xarray
    # _, index = np.unique(f['time'], return_index=True)
    # f.isel(time=index)

    # Check that the dataframe's index is sorted by date
    # if not combined.time.is_monotonic_increasing:
    #     combined = combined.sortby("time")

    # if not combined.index.is_unique:
    #     raise ValueError("Dataframe index is not unique")

    return combined
