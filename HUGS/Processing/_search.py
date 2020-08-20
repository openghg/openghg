""" Generic search functions that can be used to find data in
    the object store

"""
from enum import Enum

__all__ = [
    "get_data",
    "daterange_to_string",
    "daterange_to_string",
    "search",
    "lookup_gas_datasources",
    "lookup_footprint_datasources",
]


class RootPaths(Enum):
    DATA = "data"
    DATASOURCE = "datasource"
    INSTRUMENT = "instrument"
    SITE = "site"
    NETWORK = "network"


class DataType(Enum):
    CRDS = "CRDS"
    GC = "GC"
    FOOTPRINT = "FOOTPRINT"
    NOAA = "NOAA"
    EUROCOM = "EUROCOM"
    ThamesBarrier = "THAMESBARRIER"
    ICOS = "ICOS"


def search(
    species,
    locations,
    data_type,
    inlet=None,
    instrument=None,
    find_all=False,
    start_datetime=None,
    end_datetime=None,
):
    """ Search for gas data (optionally within a daterange)

        TODO - review this function - feel like it can be tidied and simplified

        Args:
            species (str or list): Terms to search for in Datasources
            locations (str or list): Where to search for the terms in species
            inlet (str, default=None): Inlet height such as 100m
            instrument (str, default=None): Instrument name such as picarro
            find_all (bool, default=False): Require all search terms to be satisfied
            start_datetime (datetime, default=None): Start datetime for search
            If None a start datetime of UNIX epoch (1970-01-01) is set
            end_datetime (datetime, default=None): End datetime for search
            If None an end datetime of the current datetime is set
        Returns:
            dict: List of keys of Datasources matching the search parameters
    """
    from collections import defaultdict
    from json import load
    from HUGS.Modules import Datasource
    from HUGS.Util import (get_datetime_now, get_datetime_epoch, create_daterange_str, 
                            load_object, timestamp_tzaware, get_datapath)

    if not isinstance(species, list):
        species = [species]

    if not isinstance(locations, list):
        locations = [locations]

    # Allow passing of location names instead of codes
    site_codes_json = get_datapath(filename="site_codes.json")
    with open(site_codes_json, "r") as f:
        d = load(f)
        site_codes = d["name_code"]

    updated_locations = []
    # Check locations, if they're longer than three letters do a lookup
    for loc in locations:
        if len(loc) > 3:
            try:
                site_code = site_codes[loc.lower()]
                updated_locations.append(site_code)
            except KeyError:
                raise ValueError(f"Invalid site {loc} passed")
        else:
            updated_locations.append(loc)

    locations = updated_locations

    if start_datetime is None:
        start_datetime = get_datetime_epoch()
    if end_datetime is None:
        end_datetime = get_datetime_now()

    # TODO - for now the latest dates people can access is the end of 2017
    # max_datetime = datetime_to_datetime(datetime(2017, 12, 31))
    # if end_datetime > max_datetime:
    #     end_datetime = max_datetime

    # Ensure passed datetimes are timezone aware
    start_datetime = timestamp_tzaware(start_datetime)
    end_datetime = timestamp_tzaware(end_datetime)

    # Load the required data object and get the datasource UUIDs required for metadata search
    data_type = DataType[data_type.upper()].name
    data_obj = load_object(class_name=data_type)
    datasource_uuids = data_obj.datasources()

    # Shallow load the Datasources
    datasources = [Datasource.load(uuid=uuid, shallow=True) for uuid in datasource_uuids]

    # First we find the Datasources from locations we want to narrow down our search
    location_sources = defaultdict(list)
    # If we have locations to search
    for location in locations:
        for datasource in datasources:
            if datasource.search_metadata(search_terms=location):
                location_sources[location].append(datasource)

    # This is returned to the caller
    results = defaultdict(dict)

    # With both inlet and instrument specified we bypass the ranking system
    if inlet is not None and instrument is not None:
        for site, sources in location_sources.items():
            for sp in species:
                for datasource in sources:
                    # Just match the single source here
                    if datasource.search_metadata(search_terms=[sp, site, inlet, instrument], find_all=True):
                        daterange_str = create_daterange_str(start=start_datetime, end=end_datetime)
                        # Get the data keys for the data in the matching daterange
                        in_date = datasource.in_daterange(daterange=daterange_str)

                        data_date_str = strip_dates_keys(in_date)

                        key = f"{sp}_{site}_{instrument}_{inlet}"
                        # Find the keys that match the correct data
                        results[key]["keys"] = {data_date_str: in_date}
                        results[key]["metadata"] = datasource.metadata()

        return results

    if data_type != "FOOTPRINT":
        for location, sources in location_sources.items():
            # Loop over and look for the species
            species_data = defaultdict(list)
            for datasource in sources:
                for s in species:
                    # Check the species and the daterange
                    if datasource.search_metadata(search_terms=s, find_all=find_all):
                        species_data[s].append(datasource)

            # For each location we want to find the highest ranking sources for the selected species
            for sp, sources in species_data.items():
                ranked_sources = {}

                # How to return all the sources if they're all 0?
                for source in sources:
                    rank_data = source.get_rank(start_date=start_datetime, end_date=end_datetime)

                    # With no rank set we get an empty dictionary
                    if not rank_data:
                        ranked_sources[0] = 0
                        continue

                    # Just get the highest ranked datasources and return them
                    # Find the highest ranked data from this site
                    highest_rank = sorted(rank_data.keys())[-1]

                    if highest_rank == 0:
                        ranked_sources[0] = 0
                        continue

                    ranked_sources[source.uuid()] = {"rank": highest_rank, "dateranges": rank_data[highest_rank], "source": source}

                # If it's all zeroes we want to return all sources
                if list(ranked_sources) == [0]:
                    for source in sources:
                        key = f"{sp}_{location}_{source.inlet()}"

                        daterange_str = create_daterange_str(start=start_datetime, end=end_datetime)
                        data_keys = source.in_daterange(daterange=daterange_str)

                        if not data_keys:
                            continue

                        # Get a key that covers the daterange of the actual data and not from epoch to now
                        # if no start/end datetimes are passed
                        data_date_str = strip_dates_keys(data_keys)

                        results[key]["keys"] = {data_date_str: data_keys}
                        results[key]["metadata"] = source.metadata()

                    continue
                else:
                    # TODO - find a cleaner way of doing this
                    # We might have a zero rank, delete it as we have higher ranked data
                    try:
                        del ranked_sources[0]
                    except KeyError:
                        pass

                # Otherwise iterate over the sources that are ranked and extract the keys
                for uid in ranked_sources:
                    source = ranked_sources[uid]["source"]
                    source_dateranges = ranked_sources[uid]["dateranges"]

                    key = f"{sp}_{location}_{source.inlet()}_{source.instrument()}"

                    data_keys = {}
                    # Get the keys for each daterange
                    for d in source_dateranges:
                        keys_in_date = source.in_daterange(daterange=d)
                        d = d.replace("+00:00", "")
                        if keys_in_date:
                            data_keys[d] = keys_in_date

                    if not data_keys:
                        continue

                    results[key]["keys"] = data_keys
                    results[key]["metadata"] = source.metadata()
    else:
        raise NotImplementedError("Footprint search not implemented.")

    return results


def strip_dates_keys(keys):
    """ Strips the date from a key, could this data just be read from JSON instead?
        Read dates covered from the Datasource?

        Args:
            keys (list): List of keys containing data
            data/uuid/<uuid>/<version>/2019-03-01-04:14:30+00:00_2019-05-31-20:44:30+00:00
        Returns:
            str: Daterange string
    """
    if not isinstance(keys, list):
        keys = [keys]

    keys.sort()
    start_key = keys[0]
    end_key = keys[-1]
    # Get the first and last dates from the keys in the search results
    start_date = start_key.split("/")[-1].split("_")[0].replace("+00:00", "")
    end_date = end_key.split("/")[-1].split("_")[-1].replace("+00:00", "")

    return "_".join([start_date, end_date])


def append_keys(results, search_key, keys):
    """ defaultdict(list) behaviour for keys record

        If search_key exists in results the keys list is appended
        to the list stored at that key. Otherwise a new list containing
        the keys is created and stored at that key.

        Args:
            results (dict): Results dictionary
            search_key (str): Key to use to access results
            keys (list): List of object store keys to access
        Returns:
            dict: Results dictionary
    """
    if search_key in results:
        results[search_key]["keys"].extend(keys)
    else:
        results[search_key] = {"keys": keys}

    return results


def lookup_gas_datasources(lookup_dict, gas_data, source_name, source_id):
    """ Check if the passed data exists in the lookup dict

        Args:
            lookup_dict (dict): Dictionary to search for exisiting Datasources
            gas_data (list): Gas data to process
            source_name (str): Name of course
            source_id (str, default=None): UUID of source. Not currently implemented.
        Returns:
            dict: source_name: Datasource UUID (key: value)
    """
    # If we already have data from these datasources then return that UUID
    # otherwise return False
    if source_id is not None:
        raise NotImplementedError()

    results = {}
    for species in gas_data:
        datasource_name = source_name + "_" + species
        results[species] = {}
        results[species]["uuid"] = lookup_dict.get(datasource_name, False)
        results[species]["name"] = datasource_name

    return results


def lookup_footprint_datasources(lookup_dict, source_name, source_id=None):
    """ Check if we've had data from this Datasource before

        TODO - This seems like a waste - combine this with lookup_gasDatasources ?

        Args:
            lookup_dict (dict): Dictionary to search for exisiting Datasources
            source_name (str): Name of course
            source_id (str, default=None): UUID of source. Not currently implemented.
        Returns:
            dict: source_name: Datasource UUID (key: value)
    """
    if source_id is not None:
        raise NotImplementedError()

    results = {source_name: {}}
    results[source_name]["uuid"] = lookup_dict.get(source_name, False)

    return results


def get_data(key_list):
    """ Gets data from the Datasources found by the search function

        Bypass loading the Datasource? Get both then we have metadata?

    """
    from HUGS.Modules import Datasource

    # Get the data
    # This will return a list of lists of data
    # Maybe want to do some preprocessing on this data before it comes raw out of the object store?
    # We only want the data in the correct daterange
    return [Datasource.load(key=key)._data for key in key_list]


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
#     from Acquire.ObjectStore import datetime_to_datetime as datetime_to_datetime
#     from objectstore._hugs_objstore import get_dataframe as _get_dataframe
#     from pandas import date_range as _pd_daterange

#     datetime_begin = datetime_to_datetime(datetime_begin)
#     datetime_end = datetime_to_datetime(datetime_end)

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
