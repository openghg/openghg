""" Generic search functions that can be used to find data in
    the object store

"""
from typing import Dict

__all__ = ["search"]


def search(**kwargs) -> Dict:
    # site: Union[str, List],
    # species: Optional[Union[str, List]] = None,
    # inlet: Optional[Union[str, List]] = None,
    # instrument: Optional[str] = None,
    # find_all: Optional[bool] = True,
    # start_date: Optional[Union[str, Timestamp]] = None,
    # end_date: Optional[Union[str, Timestamp]] = None,
    # data_type: Optional[str] = "timeseries",
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
    from collections import defaultdict, namedtuple
    from openghg.modules import Datasource, ObsSurface, FOOTPRINTS, Emissions
    from openghg.util import timestamp_now, timestamp_epoch, timestamp_tzaware, clean_string, daterange_from_str, split_daterange_str
    from pandas import date_range as pd_date_range
    from pandas import Timedelta as pd_Timedelta
    from addict import Dict as aDict

    # Do this here otherwise we have to produce them for every datasource
    start_date = kwargs.get("start_date")
    end_date = kwargs.get("end_date")

    if start_date is None:
        start_date = timestamp_epoch()
    else:
        start_date = timestamp_tzaware(start_date)

    if end_date is None:
        end_date = timestamp_now()
    else:
        end_date = timestamp_tzaware(end_date)

    kwargs["start_date"] = start_date
    kwargs["end_date"] = end_date

    # As we might have kwargs that are None we want to get rid of those
    search_kwargs = {k: clean_string(v) for k, v in kwargs.items() if v is not None}

    data_type = search_kwargs.get("data_type", "timeseries")

    valid_data_types = ("timeseries", "footprint", "emissions")
    if data_type not in valid_data_types:
        raise ValueError(f"{data_type} is not a valid data type, please select one of {valid_data_types}")

    # Here we want to load in the ObsSurface module for now
    if data_type == "timeseries":
        obj = ObsSurface.load()
    elif data_type == "footprint":
        obj = FOOTPRINTS.load()
    elif data_type == "emissions":
        obj = Emissions.load()

    datasource_uuids = obj.datasources()

    # Shallow load the Datasources so we can search their metadata
    datasources = (Datasource.load(uuid=uuid, shallow=True) for uuid in datasource_uuids)

    # Old search code
    # matching_sources = defaultdict(dict)
    # for datasource in datasources:
    #     if datasource.search_metadata(**search_kwargs):
    #         uid = datasource.uuid()
    #         data_keys = datasource.keys_in_daterange(start_date=start_date, end_date=end_date)
    #         matching_sources[uid]["keys"] = data_keys
    #         matching_sources[uid]["metadata"] = datasource.metadata()

    # return matching_sources

    matching_sources = defaultdict(dict)
    for datasource in datasources:
        if datasource.search_metadata(**search_kwargs):
            matching_sources[datasource.uuid()] = datasource

    # If we have the site, inlet and instrument then just return the data
    # TODO - should instrument be added here
    if {"site", "inlet", "species"} <= search_kwargs.keys():
        specific_sources = defaultdict(dict)

        for uid, datasource in matching_sources.items():
            data_keys = datasource.keys_in_daterange(start_date=start_date, end_date=end_date)
            specific_sources[uid]["keys"] = data_keys
            specific_sources[uid]["metadata"] = datasource.metadata()

        return specific_sources

    highest_ranked = aDict()

    for uid, datasource in matching_sources.items():
        # Find the site and then the ranking
        metadata = datasource.metadata()
        # Get the site inlet and species
        site = metadata["site"]
        species = metadata["species"]
        network = metadata["network"]

        rank_data = obj.get_rank(uuid=uid, start_date=start_date, end_date=end_date)

        # If this Datasource doesn't have any ranking data skip it and move on
        if not rank_data:
            continue

        # There will only be a single rank key
        rank_value = next(iter(rank_data))
        # Get the daterange this rank covers
        rank_daterange = rank_data[rank_value]

        # If we have a high rank we'll store this below
        # Adding network in here doesn't feel quite right but I can't think of a cleaner
        # way currently
        match = {"uuid": uid, "daterange": rank_daterange, "network": network}

        # Need to ensure we get all the dates covered
        if species in highest_ranked[site]:
            species_rank_data = highest_ranked[site][species]

            # If we have a higher (lower number) rank save it
            if rank_value < species_rank_data["rank"]:
                species_rank_data["rank"] = rank_value
                species_rank_data["matching"] = [match]
            # If another Datasource has the same rank for another daterange
            # we want to save that as well
            elif rank_value == species_rank_data["rank"]:
                species_rank_data["matching"].append(match)
        else:
            highest_ranked[site][species]["rank"] = rank_value
            highest_ranked[site][species]["matching"] = [match]

    # Now we have the highest ranked data the dateranges there are ranks for
    # we want to fill in the gaps with (currently) the highest inlet from that site

    data_keys = aDict()
    for site, species in highest_ranked.items():
        for sp, data in species.items():
            dateranges = (daterange_from_str(m["daterange"]) for m in data["matching"])
            combined = dateranges[0].union_many(dateranges[1:])
            search_daterange = pd_date_range(start=start_date, end=end_date)

            # Dates that are in the search daterange but aren't covered by the rank dates
            diff = search_daterange.difference(combined)

            # If we don't have any missing dates just continue
            if diff.empty:
                continue

            for rank_block_n, m in enumerate(data["matching"]):
                uuid = m["uuid"]
                daterange = m["daterange"]
                datasource = matching_sources[uuid]
                keys = datasource.keys_in_daterange_str(daterange=daterange)
                metadata = datasource.metadata().copy()
                
                # TODO - here add in the rank that this block has and the daterange that rank covers
                raise NotImplementedError()
                # metadata["rank_data"] = {"rank": }

                data_keys[site][sp]["ranked"][rank_block_n] = 0

                
            # Get the data from the Datasources we have the ranking for
            datasources = (matching_sources[m["uuid"]] for m in data["matching"])
            for
                data_keys[site][sp] = []



            date_series = diff.to_series()
            grp = date_series.diff().ne(pd_Timedelta(days=1)).cumsum()
            gaps = date_series.groupby(grp).agg(["min", "max"])
            # These tuples represent the start and end Timestamps of the gaps
            # which aren't covered by the ranked data
            timestamps = gaps[["min", "max"]].apply(tuple, axis=1).tolist()

            network = data["network"]

            # Then get the gaps
            highest_inlet = obj.get_highest_inlet(site=site, network=network, species=species)

            for n, (start, end) in enumerate(timestamps):
                data_keys[site][species][n] = search(site=site, species=sp, inlet=highest_inlet, network=network, start_date=start, end_date=end)


            # We want to get the data for the above date ranges from the highest inlet
            # From the ObsSurface class we can get the highest inlet

    # return highest_ranked

    # # Otherwise we want to find the highest ranking data for each site
    # # We just want to return the highest ranked data for each site for each species
    # ranked_data = defaultdict(dict)
    # for uid, datasource in matching_sources.items():

    #     key = "_".join((site, species))

    #     ranking = datasource.rank(start_date=start_date, end_date=end_date)

    #     ranked_data[key][inlet] = {"ranking": ranking, "uuid": uid}

    # return ranked_data

    # Then loop over the ranked data and for each get the highest ranked data that covers the dates
    # we require data for
    # highest_ranked_data = defaultdict(dict)

    # for key, inlet_data in ranked_data.items():
    #     # If we only have one rank for this data return that
    #     if len(inlet_data) == 1:
    #         inlet_height = next(iter(inlet_data))
    #         highest_ranked_data[key] = inlet_data[inlet_height]["uuid"]
    #     else:
    #         # Iterate over the inlets to find the highest ranked
    #         for inlet, specific_inlet_data in inlet_data.items():
    #             highest_rank = min(specific_inlet_data)

    #         # Get the highest ranked in let that covers these dates
    #         high_rank = min(rank_data.keys())
    #         highest_ranked_data[high_rank] = "some_data"

    # return highest_ranked_data

    # # TODO - this section of the function needs refactoring
    # # GJ - 2021-03-09
    # for location, sources in location_sources.items():
    #     # Loop over and look for the species
    #     species_data = defaultdict(list)
    #     for datasource in sources:
    #         for s in species:
    #             search_terms = [x for x in (s, location, inlet, instrument) if x is not None]
    #             # Check the species and the daterange
    #             if datasource.search_metadata(search_terms=search_terms, start_date=start_date, end_date=end_date, find_all=True):
    #                 species_data[s].append(datasource)

    #     # For each location we want to find the highest ranking sources for the selected species
    #     for sp, sources in species_data.items():
    #         ranked_sources = {}

    #         for source in sources:
    #             rank_data = source.get_rank(start_date=start_date, end_date=end_date)

    #             # With no rank set we get an empty dictionary
    #             if not rank_data:
    #                 ranked_sources[0] = 0
    #                 continue

    #             # Just get the highest ranked datasources and return them
    #             # Find the highest ranked data from this site
    #             highest_rank = sorted(rank_data.keys())[-1]

    #             if highest_rank == 0:
    #                 ranked_sources[0] = 0
    #                 continue

    #             ranked_sources[source.uuid()] = {"rank": highest_rank, "dateranges": rank_data[highest_rank], "source": source}

    #         # If it's all zeroes we want to return all sources
    #         if list(ranked_sources) == [0]:
    #             for source in sources:
    #                 key = f"{source.species()}_{source.site()}_{source.inlet()}_{source.instrument()}".lower()

    #                 data_keys = source.keys_in_daterange(start_date=start_date, end_date=end_date)

    #                 if not data_keys:
    #                     continue

    #                 results[key]["keys"] = data_keys
    #                 results[key]["metadata"] = source.metadata()

    #             continue
    #         else:
    #             # TODO - find a cleaner way of doing this
    #             # We might have a zero rank, delete it as we have higher ranked data
    #             try:
    #                 del ranked_sources[0]
    #             except KeyError:
    #                 pass

    #         # Otherwise iterate over the sources that are ranked and extract the keys
    #         for uid in ranked_sources:
    #             source = ranked_sources[uid]["source"]
    #             source_dateranges = ranked_sources[uid]["dateranges"]

    #             key = f"{source.species()}_{source.site()}_{source.inlet()}_{source.instrument()}".lower()

    #             data_keys = []
    #             # Get the keys for each daterange
    #             for d in source_dateranges:
    #                 keys_in_date = source.keys_in_daterange_str(daterange=d)
    #                 if keys_in_date:
    #                     data_keys.extend(keys_in_date)

    #             if not data_keys:
    #                 continue

    #             results[key]["keys"] = data_keys
    #             results[key]["metadata"] = source.metadata()

    # return results


# def search_footprints(
#     sites: Union[str, List[str]], domains: Union[str, List[str]], inlet: str, start_date: Timestamp, end_date: Timestamp
# ) -> Dict:
#     """Search for footprints for the given locations and inlet height.

#     Args:
#         locations: Location name or list of names
#         inlet: Inlet height
#         start_date: Start date
#         end_date: End date
#     Returns:
#         dict: Dictionary of keys keyed by location
#     """
#     from collections import defaultdict
#     from openghg.modules import Datasource, FOOTPRINTS

#     if not isinstance(sites, list):
#         sites = [sites]

#     if not isinstance(domains, list):
#         domains = [domains]

#     footprints = FOOTPRINTS.load()
#     datasource_uuids = footprints.datasources()
#     datasources = (Datasource.load(uuid=uuid, shallow=True) for uuid in datasource_uuids)

#     keys = defaultdict(dict)
#     # If we have locations to search
#     # for sites in sites:
#     for datasource in datasources:
#         for site in sites:
#             # TODO - should we iterate over the domains? Will there be the same site in multiple footprint domains?
#             search_terms = [inlet, site] + domains
#             if datasource.search_metadata(search_terms=search_terms, start_date=start_date, end_date=end_date):
#                 # Get the data keys for the data in the matching daterange
#                 in_date = datasource.keys_in_daterange(start_date=start_date, end_date=end_date)
#                 keys[site]["keys"] = in_date
#                 keys[site]["metadata"] = datasource.metadata()

#     return keys


# def search_emissions(
#     species: Union[str, List[str]],
#     sources: Union[str, List[str]],
#     domains: Union[str, List[str]],
#     high_time_res: Optional[bool] = False,
#     start_date: Optional[Union[str, Timestamp]] = None,
#     end_date: Optional[Union[str, Timestamp]] = None,
# ) -> Dict:
#     """Search for emissions for the given locations and inlet height.

#     Args:
#         locations: Location name or list of names
#         inlet: Inlet height
#         start_date: Start date
#         end_date: End date
#     Returns:
#         dict: Dictionary of keys keyed by location
#     """
#     from collections import defaultdict
#     from openghg.modules import Datasource, Emissions
#     from openghg.util import timestamp_epoch, timestamp_now, timestamp_tzaware

#     if not isinstance(species, list):
#         species = [species]

#     if not isinstance(domains, list):
#         domains = [domains]

#     if sources is not None and not isinstance(sources, list):
#         sources = [sources]

#     if start_date is None:
#         start_date = timestamp_epoch()
#     else:
#         start_date = timestamp_tzaware(start_date)

#     if end_date is None:
#         end_date = timestamp_now()
#     else:
#         end_date = timestamp_tzaware(end_date)

#     emissions = Emissions.load()
#     datasource_uuids = emissions.datasources()
#     datasources = (Datasource.load(uuid=uuid, shallow=True) for uuid in datasource_uuids)

#     gen_search_terms = []
#     if sources is not None:
#         gen_search_terms += sources
#     if high_time_res:
#         gen_search_terms.append("high_time_resolution")

#     keys = defaultdict(dict)
#     # If we have locations to search
#     # for sites in sites:
#     for datasource in datasources:
#         for sp in species:
#             for domain in domains:
#                 search_terms = [sp, domain] + gen_search_terms
#                 if datasource.search_metadata(search_terms=search_terms, start_date=start_date, end_date=end_date):
#                     # Get the data keys for the data in the matching daterange
#                     in_date = datasource.keys_in_daterange(start_date=start_date, end_date=end_date)
#                     key = "_".join(search_terms)
#                     keys[key]["keys"] = in_date
#                     keys[key]["metadata"] = datasource.metadata()

#     return keys


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
