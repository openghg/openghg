""" Handles the recombination of dataframes stored in the object store
    into the data requested by the user

"""
__all__ = ["get_datasources", "get_dataframes",
           "combine_sections", "convert_to_netcdf",
           "recombine_sections"]

def get_datasources(bucket, uuid_list):
    """ Get the Datasources containing the data from the object 
        store for recombination of data

        Args:
            key_list (list): List of keys for data in the object store
        Returns:
            list: List of Datasource objects
    """
    from HUGS.Modules import Datasource as _Datasource

    return [_Datasource.load(bucket=bucket, uuid=uid) for uid in uuid_list]


# This might be unnecessary
def get_dataframes(datasources):
    """ Get the data from the Datasources and return the dataframes

        Args:
            datasources (list): List of datasources
        Returns:
            list: List of Pandas.Dataframes
    """
    x = [datasource._data for datasource in datasources]

    return False

def get_data(data_keys):
    """ Get the data stored in data_keys from the object store

    """
    from HUGS.ObjectStore import read_object as _read_object
    from HUGS.ObjectStore import get_bucket as _get_bucket

    bucket = _get_bucket()

    data = [_read_object(bucket=bucket, key=key) for key in data_keys]


def recombine_sections(data_keys):
    """ Combines separate dataframes into a single dataframe for
        processing to NetCDF for output

        Args:
            data_keys (list): List of keys for data stored in the object store
        Returns:
            Pandas.Dataframe: Combined dataframes
    """
    from pandas import concat as _concat
    from HUGS.ObjectStore import get_object as _get_object
    from HUGS.ObjectStore import get_bucket as _get_bucket
    from HUGS.Modules import Datasource as _Datasource

    bucket = _get_bucket()
    sections = [_Datasource.load_dataframe(bucket=bucket, key=key) for key in data_keys]

    combined = _concat(sections, axis="rows")

    # Check that the dataframe is sorted by date
    if not combined.index.is_monotonic_increasing:
        combined = combined.sort_index()

    # print("Combined : ", combined)

    # print(sections)

    # combined = []
    # # Combine the dataframes and create a frame of their indices
    # for section in sections:
    #     combo = _concat(section, axis="rows")
    #     combined.append(combo)

    # # Take the datetime column for checking
    # complete = combined[0].iloc[:, :1]

    # print(complete)

    # for d in combined:
    #     if len(d.index) != len(complete.index):
    #         raise ValueError("Mismatch in timeframe and dataframes index length")
    #     complete = _concat([complete, d], axis="columns")

    return combined

def combine_sections(sections):
    """ Combines separate dataframes into a single dataframe for
        processing to NetCDF for output

        TODO - An order for recombination of sections

        Args:
            sections (list): List of list of dataframes for each segment 
            for recombination
        Returns:
            Pandas.Dataframe: Combined dataframes
    """
    from pandas import concat as _concat

    combined = []
    # Combine the dataframes and create a frame of their indices
    for section in sections:
        combo = _concat(section, axis="rows")
        combined.append(combo)

    complete = combined[0].iloc[:, :1]

    for d in combined:
        if len(d.index) != len(complete.index):
            raise ValueError("Mismatch in timeframe and dataframes index length")
        complete = _concat([complete, d], axis="columns")

    return complete


def convert_to_netcdf(dataframe):
    """ Converts the passed dataframe to netcdf, performs checks
        and returns

        TODO - in memory return of a NetCDF file as a bytes object

        Args:
            dataframe (Pandas.Dataframe): Dataframe for convesion
        Returns:
            str: Name of file written
    """
    from Acquire.ObjectStore import get_datetime_now_to_string as _get_datetime_now_to_string

    filename = "crds_output_%s.nc" % _get_datetime_now_to_string()

    ds = dataframe.to_xarray().to_netcdf(filename)

    return filename

