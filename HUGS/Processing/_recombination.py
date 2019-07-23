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
    from pandas import concat as _concat
    from HUGS.ObjectStore import get_object as _get_object
    from HUGS.ObjectStore import get_bucket as _get_bucket
    from HUGS.Modules import Datasource as _Datasource

    bucket = _get_bucket()

    data = [_Datasource.load_dataframe(bucket=bucket, key=k) for k in data_keys]

    combined = _concat(data, axis="rows")
    # Check that the dataframe's index is sorted by date
    if not combined.index.is_monotonic_increasing:
        combined = combined.sort_index()
    
    return combined
