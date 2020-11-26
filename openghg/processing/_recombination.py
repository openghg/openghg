""" Handles the recombination of dataframes stored in the object store
    into the data requested by the user

"""
__all__ = ["recombine_sections"]


def recombine_sections(data_keys):
    """ Combines separate dataframes into a single dataframe for
        processing to NetCDF for output

        Args:
            data_keys (list): Dictionary of object store keys keyed by search
            term
        Returns:
            Pandas.Dataframe or list: Combined dataframes
    """
    # from pandas import concat as _concat
    from xarray import concat as xr_concat
    from openghg.objectstore import get_bucket
    from openghg.modules import Datasource

    bucket = get_bucket()

    data = [Datasource.load_dataset(bucket=bucket, key=k) for k in data_keys]

    combined = xr_concat(data, dim="time")

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
