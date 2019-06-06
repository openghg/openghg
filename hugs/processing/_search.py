""" Generic search functions that can be used to find data in
    the object store

"""
from enum import Enum as _Enum

# __all__ = ["string_to_daterange", "daterange_to_string",
#            "parse_date_time", "get_objects"]

class RootPaths(_Enum):
    DATA = "data"
    DATASOURCE = "datasource"
    INSTRUMENT = "instrument"
    SITE = "site"
    NETWORK = "network"


def search_store(bucket, data_uuids, root_path, start_datetime, end_datetime):
        """ Get all values stored in the object store

            Args:
                bucket (dict): Bucket holding data
                data_uuids (list): List of UUIDs to search
                root_path (str): Select from the enum RootPaths
                For DataSources: datasource
                For Instruments: instrument etc
                datetime_begin (datetime): Start of datetime range
                datetime_end (datetime): End of datetime range
            Returns:
                list: A list of keys for the found data

        """
        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from Acquire.ObjectStore import datetime_to_datetime as _datetime_to_datetime
        from objectstore._hugs_objstore import get_dataframe as _get_dataframe
        from objectstore._hugs_objstore import get_object_names as _get_object_names
        from pandas import date_range as _pd_daterange

        start_datetime = _datetime_to_datetime(start_datetime)
        end_datetime = _datetime_to_datetime(end_datetime)

        # Something like this?
        # freq = "YS"
        # resolution = "%Y"
        # if start_datetime.month != 0 and end_datetime.month != 0:
        #     resolution += "%m"
        #     freq = "MS"
        # if start_datetime.day != 0 and end_datetime.day != 0:
        #     resolution += "%d"
        #     freq = "D"
        # if start_datetime.hour != 0 and end_datetime.hour != 0:
        #     resolution += "%h"
        #     freq = "H"

        # At the moment just have years
        # daterange = _pd_daterange(start=start_datetime, end=end_datetime, freq="Y")

        # path = RootPaths[root_path.upper()]

        # TODO - Change this to work with enums?
        path = "data"

        # Get the UUIDs for the data
        # data_uuids = [d._uuid for d in self._datasources]

        # If we know the UUIDs we have read the dateranges from the metadata stored
        # and return the data
        # This will have to be changed again when the dataframes are split up

        # Where to look
        keys = []
        for uuid in data_uuids:
            prefix = "%s/uuid/%s" % ("data", uuid)
            # Get the keys that start with this and read the daterange from the returned value
            keys.extend(_get_object_names(bucket=bucket, prefix=prefix))

        # The data to get
        # TODO - once segmentation by date is functional this
        # can be extended to include the dateranges properly
        data_uuids = []
        # Get the daterange
        for key in keys:
            if in_daterange(key, start_datetime, end_datetime):
                data_uuids.append(key)
        return data_uuids


def in_daterange(key, start_search, end_search):
    """ Does this key contain data in the daterange we want?

        Args:
            key (str): Key for data
            daterange (tuple (datetime, datetime)): Daterange as start and end datetime objects
        Return:
            bool: True if key within daterange
    """
    start_key, end_key = key_to_daterange(key)

    if start_key >= start_search and end_key <= end_search:
        return True
    else:
        return False


def key_to_daterange(key):
    """ Takes a dated key and returns two datetimes for the start and 
        end datetimes for the data

        Args:
            key (str): Key for data in the object store
        Returns:
            tuple (datetime, datetime): Datetimes for start and end of data

    """
    from Acquire.ObjectStore import string_to_datetime as _string_to_datetime

    end_key = key.split("/")[-1]
    dates = end_key.split("_")

    if len(dates) > 2:
        raise ValueError("Invalid date string")

    start = _string_to_datetime(dates[0])
    end = _string_to_datetime(dates[1])

    return start, end


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


# def get_objects(bucket, root_path, datetime_begin, datetime_end):
#     """ Get all values stored in the object store

#         Args:  
#             bucket (dict): Bucket holding data
#             root_path (str): Select from the enum RootPaths
#             For DataSources: datasource
#             For Instruments: instrument etc
#             datetime_begin (datetime): Start of datetime range
#             datetime_end (datetime): End of datetime range
#         Returns:
#             list: A list of Pandas.Dataframes

#     """
#     from Acquire.ObjectStore import ObjectStore as _ObjectStore
#     from Acquire.ObjectStore import datetime_to_datetime as _datetime_to_datetime
#     from objectstore._hugs_objstore import get_dataframe as _get_dataframe
#     from pandas import date_range as _pd_daterange

#     datetime_begin = _datetime_to_datetime(datetime_begin)
#     datetime_end = _datetime_to_datetime(datetime_end)

#     daterange = _pd_daterange(datetime_begin, datetime_end)

#     freq = "YS"
#     resolution = "%Y"
#     if start_datetime.month != 0 and end_datetime.month != 0:
#         resolution += "%m"
#         freq = "MS"
#     if start_datetime.day != 0 and end_datetime.day != 0:
#         resolution += "%d"
#         freq = "D"
#     if start_datetime.hour != 0 and end_datetime.hour != 0:
#         resolution += "%h"
#         freq = "H"

#     keys = []

#     path = RootPaths[root_path.upper()]

#     for date in daterange:
#         date_string = date.strftime(resolution)
#         prefix = "%s/%s/%s" % (path, uuid, date_string)

#         # datakeys = 



#     # Find the keys that are valid
#     for year in range(year_begin, year_end+1):
#         prefix = "%s/%s/%s" % (path, self._uuid, year)

#         datakeys = _ObjectStore.get_all_object_names(bucket=bucket, prefix=prefix)

#         # Check the end date of the data
#         for datakey in datakeys:
#             _, end = string_to_daterange(datakey.split("_")[-1])

#             if end.year < year_end:
#                 keys.append(datakey)

#     # List to store dataframes
#     values = []

#     for key in keys:
#         values.append(_get_dataframe(bucket=bucket, key=key))

#     return values
