""" Generic search functions that can be used to find data in
    the object store

"""
from enum import Enum as _Enum

class RootPaths(_Enum):
    DATASOURCE = "datasource"
    INSTRUMENT = "instrument"
    SITE = "site"
    NETWORK = "network"

def get_values(self, bucket, root_path, datetime_begin, datetime_end):
    """ Get all values for this Datasource stored in the object store

        Args:  
            bucket (dict): Bucket holding data
            root_path (str): Select from the enum RootPaths
            For DataSources: datasource
            For Instruments: instrument etc
            datetime_begin (datetime): Start of datetime range
            datetime_end (datetime): End of datetime range
        Returns:
            list: A list of Pandas.Dataframes

    """
    from Acquire.ObjectStore import ObjectStore as _ObjectStore
    from Acquire.ObjectStore import datetime_to_datetime as _datetime_to_datetime

    # Ensure that datetime is valid
    datetime_begin = _datetime_to_datetime(datetime_begin)
    datetime_end = _datetime_to_datetime(datetime_end)

    year_begin = datetime_begin.year
    year_end = datetime_end.year

    keys = []

    path = RootPaths[root_path.upper()]
    # Find the keys that are valid
    for year in range(year_begin, year_end+1):
        prefix = "%s/%s/%s" % (path, self._uuid, year)

        datakeys = _ObjectStore.get_all_object_names(bucket=bucket, prefix=prefix)

        # Check the end date of the data
        for datakey in datakeys:
            start, end = _string_to_daterange(datakey.split("_")[-1])

            if end.year < year_end:
                keys.append(datakey)

    # List to store dataframes
    values = []

    for key in keys:
        values.append(get_dataframe(bucket=bucket, key=key))

    return values
