""" Generic search functions that can be used to find data in
    the object store

"""
from enum import Enum as _Enum

__all__ = ["string_to_daterange", "daterange_to_string",
           "parse_date_time", "get_objects"]

class RootPaths(_Enum):
    DATASOURCE = "datasource"
    INSTRUMENT = "instrument"
    SITE = "site"
    NETWORK = "network"

def string_to_daterange(s):
    """ Converts a daterange string from the type used with the
        object store to a datetime
        Start_end
        YYYYMMDD_YYYYMMDD

        Args:
            s (str): String containing daterange
        Returns:
            tuple (datetime, datetime): Datetime objects for daterange
    """
    import datetime as _datetime

    parts = s.split("_")
    start = _datetime.datetime.strptime(parts[0], "%Y%m%d")
    end = _datetime.datetime.strptime(parts[1], "%Y%m%d")

    return start, end

def daterange_to_string(start, end):
    """ Creates a string from the start and end datetime
    objects. Used for production of the key
    to store segmented data in the object store.

    Args:
        start (datetime): Start datetime
        end (datetime): End datetime
    Returns:
        str: Daterange formatted as start_end
        YYYYMMDD_YYYYMMDD
        Example: 20190101_20190201
    """

    start_fmt = start.strftime("%Y%m%d")
    end_fmt = end.strftime("%Y%m%d")

    return start_fmt + "_" + end_fmt

def parse_date_time(date, time):
    """ This function takes two strings and creates a datetime object 
        
        Args:
            date (str): The date in a YYMMDD format
            time (str): The time in the format hhmmss
            Example: 104930 for 10:49:30
        Returns:
            datetime: Datetime object

    """
    import datetime as _datetime

    if len(date) != 6:
        raise ValueError("Incorrect date format, should be YYMMDD")
    if len(time) != 6:
        raise ValueError("Incorrect time format, should be hhmmss")

    combined = date + time

    return _datetime.datetime.strptime(combined, "%y%m%d%H%M%S")


def get_objects(self, bucket, root_path, datetime_begin, datetime_end):
    """ Get all values stored in the object store

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
    from hugs_objstore import get_dataframe as _get_dataframe

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
            _, end = string_to_daterange(datakey.split("_")[-1])

            if end.year < year_end:
                keys.append(datakey)

    # List to store dataframes
    values = []

    for key in keys:
        values.append(_get_dataframe(bucket=bucket, key=key))

    return values
