""" Generic search functions that can be used to find data in
    the object store

"""
from enum import Enum as _Enum

__all__ = ["get_data",  "in_daterange",
           "key_to_daterange", "daterange_to_string", 
           "daterange_to_string", "search"]


class RootPaths(_Enum):
    DATA = "data"
    DATASOURCE = "datasource"
    INSTRUMENT = "instrument"
    SITE = "site"
    NETWORK = "network"


class DataType(_Enum):
    CRDS = "CRDS"
    GC = "GC"


def search(search_terms, locations, data_type, require_all=False, start_datetime=None, end_datetime=None):
    """ Search for gas data (optionally within a daterange)
    
        Args:
            search_terms (string or list): Terms to search for in Datasources
            locations (string or list): Where to search for the terms in search_terms
            require_all (bool, default=False): Require all search terms to be satisfied
            start_datetime (datetime, default=None): Start datetime for search
            If None a start datetime of UNIX epoch (1970-01-01) is set
            end_datetime (datetime, default=None): End datetime for search
            If None an end datetime of the current datetime is set
        Returns:
            dict: List of keys of Datasources matching the search parameters
    """
    from HUGS.ObjectStore import get_object_names as _get_object_names
    from HUGS.ObjectStore import get_local_bucket as _get_local_bucket
    from HUGS.Modules import Datasource as _Datasource
    from HUGS.Util import get_datetime_epoch as _get_datetime_epoch
    from HUGS.Util import get_datetime_now as _get_datetime_now
    from HUGS.Util import load_object as _load_object

    from collections import defaultdict as _defaultdict

    if start_datetime is None:
        start_datetime = _get_datetime_epoch()
    if end_datetime is None:
        end_datetime = _get_datetime_now()

    search_prefix = "%s/uuid/" % data_type
    bucket = _get_local_bucket()

    # TODO - method to load different types in here for search
    # Maybe just an if else for now?
    data_type = DataType[data_type.upper()]
    # Get the objects that contain the Datasources
    object_list = _get_object_names(bucket=bucket, prefix=search_prefix)
    object_uuid = object_list[0].split("/")[-1]

    if len(object_list) == 0:
        raise ValueError("No " + data_type.name + " object found.")
    if len(object_list) > 1:
        raise ValueError("More than one " + data_type.name + " object found.")

    data_obj = _load_object(data_type.name)
    # Get the UUIDs of the Datasources associated with the object
    datasource_uuids = data_obj.datasources()

    # First check if the uuids we have are in the list of known and valid Datasources
    # This could be an object has a quick lookup data structure so we don't need to load
    # in the datasources and search their keys
    # TODO - implement lookup tables?
    datasources = [_Datasource.load(uuid=uuid, shallow=True) for uuid in datasource_uuids]

    if not isinstance(search_terms, list):
        search_terms = [search_terms]

    if not isinstance(locations, list):
        locations = [locations]
    
    # Here can return a single key for each search term
    # How to seach for 3 different sites
    # bilsdale, heathfield, tacolneston
    # Between 2016 - 2018
    # search terms bsd, hfd, tac

    # Just want to return a single composite key of all search terms
    if require_all:
        single_key = "_".join(sorted(search_terms))
        
    # First we find the Datasources from locations we want to narrow down our search
    location_sources = _defaultdict(list)
    for location in locations:
        for datasource in datasources:
            if datasource.search_labels(location):
                location_sources[location].append(datasource)

    # Next we search these keys for the search terms we require
    keys = _defaultdict(list)
    # Search for the search terms in the locations that we want
    for search_term in search_terms:
        for location in location_sources:
            for datasource in location_sources[location]:
                if datasource.search_labels(search_term):
                    prefix = "data/uuid/%s" % datasource.uuid()
                    data_list = _get_object_names(bucket=bucket, prefix=prefix)
                    # Get the Dataframes that are within the dates we are searching for
                    in_date = [d for d in data_list if in_daterange(d, start_datetime, end_datetime)]

                    if require_all:
                        remaining_terms = [datasource.search_labels(term) for term in search_terms if term != search_term]
                        # Check if we got all Trues for the other search terms
                        if all(remaining_terms):
                            keys[search_key].extend(in_date)
                    else:
                        keys[location].extend(in_date)

    return keys

    # for search_term in search_terms:
    #     for datasource in datasources:
    #         # Check the Datasource labels for the search term
    #         if datasource.search_labels(search_term):
    #             prefix = "data/uuid/%s" % datasource.uuid()
    #             data_list = _get_object_names(bucket=bucket, prefix=prefix)
    #             # Get the Dataframes that are within the dates we are searching for
    #             in_date = [d for d in data_list if in_daterange(d, start_datetime, end_datetime)]

    #             if require_all:
    #                 # Check if this Datasource also contains all the other terms we're searching for
    #                 # and get True/False values
    #                 search_key = single_key
    #                 remaining_terms = [datasource.search_labels(term) for term in search_terms if term != search_term]
    #                 # Check if we got all Trues for the other search terms
    #                 if all(remaining_terms):
    #                     keys[search_key].extend(in_date)
    #             else:
    #                 search_key = search_term + "_" + datasource.species()
    #                 keys[search_key].extend(in_date)
                   
    # Remove the empty keys
    # keys = {k: v for k,v in keys.items() if len(keys[k]) != 0}

    return keys


def get_data(key_list):
    """ Gets data from the Datasources found by the search function

        Bypass loading the Datasource? Get both then we have metadata?

    """
    from HUGS.Modules import Datasource as _Datasource
    # Get the data
    # This will return a list of lists of data
    # Maybe want to do some preprocessing on this data before it comes raw out of the object store?
    # We only want the data in the correct daterange
    return [_Datasource.load(key=key)._data for key in key_list]



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
