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


def gas_search(gas_name, meas_type, start_datetime=None, end_datetime=None):
    """ Search for gas data (optionally within a daterange)
    
        Load instruments from the correct network ?
        Read labels of the Datasources in the Instruments and record ones that match
        Get a list of UUIDs

        Return list of UUIDs of matching dataframes / datasources?

        WIP

    """
    from objectstore import get_object_names as _get_object_names
    from objectstore import get_local_bucket as _get_local_bucket
    from modules import Instrument
    from modules import CRDS
    from util import get_datetime_epoch as _get_datetime_epoch
    from util import get_datetime_now as _get_datetime_now

    # TODO - This feels clunky
    if start_datetime is None:
        start_datetime = _get_datetime_epoch()
    if end_datetime is None:
        end_datetime = _get_datetime_now()

    search_prefix = "%s/uuid/" % meas_type
    bucket = _get_local_bucket()

    crds_list = _get_object_names(bucket=bucket, prefix=search_prefix)
    crds_uuid = crds_list[0].split("/")[-1]

    crds = CRDS.load(bucket=bucket, uuid=crds_uuid)

    # Get instrument UUIDs
    instrument_uuids = list(crds.get_instruments())
    instruments = [Instrument.load(uuid=uuid, shallow=True) for uuid in instrument_uuids]

    keys = []
    for inst in instruments:
        # Search labels of Instrument for Datasources that hold the gas data we want
        labels = inst.get_labels()
        # Loop over the keys 
        for k in labels.keys():
            # Need to query the object store for the keys
            # At the moment just use data? Genericise the search and pass argument somehow?
            if gas_name in list(labels[k].values()):
                # Get all the data keys for this object
                prefix = "data/uuid/%s" % k
                data_list = _get_object_names(bucket=bucket, prefix=prefix)
                # Only keep the keys that are within the daterange we want
                in_date = [d for d in data_list if in_daterange(d, start_datetime, end_datetime)]
            
                keys.extend(in_date)

    return keys

def get_data(key_list):
    """ Gets data from the Datasources found by the search function

        Bypass loading the Datasource? Get both then we have metadata?

    """
    # Get the data
    # This will return a list of lists of data
    # Maybe want to do some preprocessing on this data before it comes raw out of the object store?
    # We only want the data in the correct daterange
    return [Datasource.load(key=key)._data for key in key_list]


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
