""" Generic search functions that can be used to find data in
    the object store

"""
from pandas import Timestamp
from typing import Dict, List, Optional, Union

__all__ = ["search", "search_footprints"]


def search(
    locations: Union[str, List],
    species: Optional[Union[str, List]] = None,
    inlet: Optional[Union[str, List]] = None,
    instrument: Optional[str] = None,
    find_all: Optional[bool] = True,
    start_date: Optional[Union[str, Timestamp]] = None,
    end_date: Optional[Union[str, Timestamp]] = None,
    data_type: Optional[str] = "timeseries",
) -> Dict:
    """Search for observations data

    Args:
        species: Terms to search for in Datasources
        locations: Where to search for the terms in species
        inlet: Inlet height such as 100m
        instrument: Instrument name such as picarro
        find_all: Require all search terms to be satisfied
        start_date: Start datetime for search.
        If None a start datetime of UNIX epoch (1970-01-01) is set
        end_date: End datetime for search.
        If None an end datetime of the current datetime is set
    Returns:
        dict: List of keys of Datasources matching the search parameters
    """
    from collections import defaultdict
    from json import load
    from openghg.modules import Datasource, ObsSurface
    from openghg.util import timestamp_now, timestamp_epoch, timestamp_tzaware, get_datapath

    # if species is not None and not isinstance(species, list):
    if not isinstance(species, list):
        species = [species]

    if not isinstance(locations, list):
        locations = [locations]

    if data_type in ["footprint", "footprints"]:
        return search_footprints(
            locations=locations,
            species=species,
            inlet=inlet,
            find_all=find_all,
            start_date=start_date,
            end_date=end_date,
        )

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

    if start_date is None:
        start_date = timestamp_epoch()
    if end_date is None:
        end_date = timestamp_now()

    # Ensure passed datetimes are timezone aware
    start_date = timestamp_tzaware(start_date)
    end_date = timestamp_tzaware(end_date)

    # Here we want to load in the ObsSurface module for now
    obs = ObsSurface.load()
    datasource_uuids = obs.datasources()

    # Shallow load the Datasources so we can search their metadata
    datasources = (Datasource.load(uuid=uuid, shallow=True) for uuid in datasource_uuids)

    # First we find the Datasources from locations we want to narrow down our search
    location_sources = defaultdict(list)
    # If we have locations to search
    for location in locations:
        for datasource in datasources:
            if datasource.search_metadata(search_terms=location, start_date=start_date, end_date=end_date):
                location_sources[location].append(datasource)

    # This is returned to the caller
    results = defaultdict(dict)
    # With both inlet and instrument specified we bypass the ranking system
    if inlet is not None and instrument is not None:
        for site, sources in location_sources.items():
            for sp in species:
                search_terms = [x for x in (sp, site, inlet, instrument) if x is not None]
                for datasource in sources:
                    # Just match the single source here
                    if datasource.search_metadata(
                        search_terms=search_terms, start_date=start_date, end_date=end_date, find_all=True
                    ):
                        # Get the data keys for the data in the matching daterange
                        data_keys = datasource.keys_in_daterange(start_date=start_date, end_date=end_date)

                        key = f"{datasource.species()}_{site}_{inlet}_{instrument}".lower()

                        # Find the keys that match the correct data
                        results[key]["keys"] = data_keys
                        results[key]["metadata"] = datasource.metadata()

        return results

    for location, sources in location_sources.items():
        # Loop over and look for the species
        species_data = defaultdict(list)
        for datasource in sources:
            for s in species:
                search_terms = [x for x in (s, location, inlet, instrument) if x is not None]
                # Check the species and the daterange
                if datasource.search_metadata(search_terms=search_terms, start_date=start_date, end_date=end_date, find_all=True):
                    species_data[s].append(datasource)

        # For each location we want to find the highest ranking sources for the selected species
        for sp, sources in species_data.items():
            ranked_sources = {}

            for source in sources:
                rank_data = source.get_rank(start_date=start_date, end_date=end_date)

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

                    data_keys = source.keys_in_daterange(start_date=start_date, end_date=end_date)

                    if not data_keys:
                        continue

                    results[key]["keys"] = data_keys
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

                data_keys = []
                # Get the keys for each daterange
                for d in source_dateranges:
                    keys_in_date = source.keys_in_daterange_str(daterange=d)
                    if keys_in_date:
                        data_keys.extend(keys_in_date)

                if not data_keys:
                    continue

                results[key]["keys"] = data_keys
                results[key]["metadata"] = source.metadata()

    return results


def search_footprints(locations: Union[str, List[str]], inlet: str, start_date: Timestamp, end_date: Timestamp) -> Dict:
    """Search for footprints for the given locations and inlet height.

    Args:
        locations: Location name or list of names
        inlet: Inlet height
        start_date: Start date
        end_date: End date
    Returns:
        dict: Dictionary of keys keyed by location
    """
    from collections import defaultdict
    from openghg.modules import Datasource, FOOTPRINTS

    if not isinstance(locations, list):
        locations = [locations]

    footprints = FOOTPRINTS.load()
    datasource_uuids = footprints.datasources()
    datasources = (Datasource.load(uuid=uuid, shallow=True) for uuid in datasource_uuids)

    keys = defaultdict(dict)
    # If we have locations to search
    # for location in locations:
    for datasource in datasources:
        for location in locations:
            if datasource.search_metadata(search_terms=[inlet, location], start_date=start_date, end_date=end_date):
                # Get the data keys for the data in the matching daterange
                in_date = datasource.keys_in_daterange(start_date=start_date, end_date=end_date)
                keys[location]["keys"] = in_date
                keys[location]["metadata"] = datasource.metadata()

    return keys


def _strip_dates_keys(keys):
    """Strips the date from a key, could this data just be read from JSON instead?
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
