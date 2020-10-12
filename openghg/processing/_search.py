""" Generic search functions that can be used to find data in
    the object store

"""
__all__ = [
    "search",
    "lookup_gas_datasources",
    "lookup_footprint_datasources",
]


def search(
    locations,
    species=None,
    inlet=None,
    instrument=None,
    find_all=True,
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
            find_all (bool, default=True): Require all search terms to be satisfied
            start_datetime (datetime, default=None): Start datetime for search
            If None a start datetime of UNIX epoch (1970-01-01) is set
            end_datetime (datetime, default=None): End datetime for search
            If None an end datetime of the current datetime is set
        Returns:
            dict: List of keys of Datasources matching the search parameters
    """
    from collections import defaultdict
    from json import load
    from openghg.modules import Datasource, ObsSurface
    from openghg.util import (get_datetime_now, get_datetime_epoch, create_daterange_str, 
                            timestamp_tzaware, get_datapath)

    # if species is not None and not isinstance(species, list):
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

    # Ensure passed datetimes are timezone aware
    start_datetime = timestamp_tzaware(start_datetime)
    end_datetime = timestamp_tzaware(end_datetime)

    # Here we want to load in the ObsSurface module for now
    obs = ObsSurface.load()
    datasource_uuids = obs.datasources()

    # Shallow load the Datasources so we can search their metadata
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

                        data_date_str = _strip_dates_keys(in_date)

                        key = f"{sp}_{site}_{inlet}_{instrument}".lower()

                        # Find the keys that match the correct data
                        results[key]["keys"] = {data_date_str: in_date}
                        results[key]["metadata"] = datasource.metadata()

        return results

    for location, sources in location_sources.items():
        # Loop over and look for the species
        species_data = defaultdict(list)
        for datasource in sources:
            for s in species:
                search_terms = [x for x in (s, location, inlet, instrument) if x is not None]
                # Check the species and the daterange
                if datasource.search_metadata(search_terms=search_terms, find_all=True):
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
                    key = f"{source.species()}_{source.site()}_{source.inlet()}_{source.instrument()}".lower()

                    daterange_str = create_daterange_str(start=start_datetime, end=end_datetime)
                    data_keys = source.in_daterange(daterange=daterange_str)

                    if not data_keys:
                        continue

                    # Get a key that covers the daterange of the actual data and not from epoch to now
                    # if no start/end datetimes are passed
                    data_date_str = _strip_dates_keys(data_keys)

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

                key = f"{source.species()}_{source.site()}_{source.inlet()}_{source.instrument()}".lower()

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

    return results


def _strip_dates_keys(keys):
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


def lookup_gas_datasources(lookup_dict, gas_data, source_name):
    """ Check if the passed data exists in the lookup dict

        Args:
            lookup_dict (dict): Dictionary to search for exisiting Datasources
            gas_data (list): Gas data to process
            source_name (str): Name of course
            source_id (str, default=None): UUID of source. Not currently implemented.
        Returns:
            dict: source_name: Datasource UUID (key: value)
    """
    import warnings
    # If we already have data from these datasources then return that UUID
    # otherwise return False
    warnings.warn("This function will be removed in a future release", category=DeprecationWarning)

    results = {}
    for species in gas_data:
        datasource_name = source_name + "_" + species
        results[species] = {}
        results[species]["uuid"] = lookup_dict.get(datasource_name, False)
        results[species]["name"] = datasource_name

    return results


def lookup_footprint_datasources(lookup_dict, source_name):
    """ Check if we've had data from this Datasource before

        TODO - This seems like a waste - combine this with lookup_gasDatasources ?

        Args:
            lookup_dict (dict): Dictionary to search for exisiting Datasources
            source_name (str): Name of course
            source_id (str, default=None): UUID of source. Not currently implemented.
        Returns:
            dict: source_name: Datasource UUID (key: value)
    """
    results = {source_name: {}}
    results[source_name]["uuid"] = lookup_dict.get(source_name, False)

    return results
