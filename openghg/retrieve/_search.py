""" Generic search functions that can be used to find data in
    the object store

"""

from typing import Dict, Union

__all__ = ["search"]


# TODO
# GJ - 20210721 - I think using kwargs here could lead to errors so we could have different user
# facing interfaces to a more general search function, this would also make it easier to enforce types
def search(**kwargs):  # type: ignore
    """Search for observations data. Any keyword arguments may be passed to the
    the function and these keywords will be used to search the metadata associated
    with each Datasource.

    Example / commonly used arguments are given below.

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
        skip_ranking: If True skip ranking system, defaults to False
    Returns:
        dict: List of keys of Datasources matching the search parameters
    """
    from addict import Dict as aDict
    from copy import deepcopy
    from itertools import chain as iter_chain
    from pandas import Timedelta as pd_Timedelta

    from openghg.store import ObsSurface, Footprints, Emissions, EulerianModel
    from openghg.store.base import Datasource

    from openghg.util import (
        timestamp_now,
        timestamp_epoch,
        timestamp_tzaware,
        clean_string,
        find_daterange_gaps,
        split_daterange_str,
        load_json,
    )
    from openghg.dataobjects import SearchResults

    # Get a copy of kwargs as we make some modifications below
    kwargs_copy = deepcopy(kwargs)

    # Do this here otherwise we have to produce them for every datasource
    start_date = kwargs.get("start_date")
    end_date = kwargs.get("end_date")

    if start_date is None:
        start_date = timestamp_epoch()
    else:
        start_date = timestamp_tzaware(start_date) + pd_Timedelta("1s")

    if end_date is None:
        end_date = timestamp_now()
    else:
        end_date = timestamp_tzaware(end_date) - pd_Timedelta("1s")

    kwargs_copy["start_date"] = start_date
    kwargs_copy["end_date"] = end_date

    skip_ranking = kwargs_copy.get("skip_ranking", False)

    try:
        del kwargs_copy["skip_ranking"]
    except KeyError:
        pass

    # As we might have kwargs that are None we want to get rid of those
    search_kwargs = {k: clean_string(v) for k, v in kwargs_copy.items() if v is not None}

    # Speices translation

    species = search_kwargs.get("species")

    if species is not None:
        if not isinstance(species, list):
            species = [species]

        translator = load_json("species_translator.json")

        updated_species = []

        for s in species:
            updated_species.append(s)

            try:
                translated = translator[s]
            except KeyError:
                pass
            else:
                updated_species.extend(translated)

        search_kwargs["species"] = updated_species

    data_type = search_kwargs.get("data_type", "timeseries")

    valid_data_types = ("timeseries", "footprints", "emissions", "eulerian_model")
    if data_type not in valid_data_types:
        raise ValueError(f"{data_type} is not a valid data type, please select one of {valid_data_types}")

    # Assume we want timeseries data
    obj: Union[ObsSurface, Footprints, Emissions, EulerianModel] = ObsSurface.load()

    if data_type == "footprints":
        obj = Footprints.load()
    elif data_type == "emissions":
        obj = Emissions.load()
    elif data_type == "eulerian_model":
        obj = EulerianModel.load()

    datasource_uuids = obj.datasources()

    # Shallow load the Datasources so we can search their metadata
    datasources = (Datasource.load(uuid=uuid, shallow=True) for uuid in datasource_uuids)

    # For the time being this will return a dict until we know how best to represent
    # the footprints and emissions results in a SearchResult object
    if data_type in {"emissions", "footprints", "eulerian_model"}:
        sources: Dict = aDict()
        for datasource in datasources:
            if datasource.search_metadata(**search_kwargs):
                uid = datasource.uuid()
                sources[uid]["keys"] = datasource.keys_in_daterange(start_date=start_date, end_date=end_date)
                sources[uid]["metadata"] = datasource.metadata()

        return sources

    # Find the Datasources that contain matching metadata
    matching_sources = {d.uuid(): d for d in datasources if d.search_metadata(**search_kwargs)}

    # TODO - Update this as it only uses the ACRG repo JSON at the moment
    # Check if this site only has one inlet, if so skip ranking
    # if "site" in search_kwargs:
    #     site = search_kwargs["site"]
    #     if not isinstance(site, list) and not multiple_inlets(site=site):
    #         skip_ranking = True

    # If there isn't *any* ranking data at all, skip all the ranking functionality
    if not obj._rank_data:
        skip_ranking = True

    # If only one datasource has been returned, skip all the ranking functionality
    if len(matching_sources) == 1:
        skip_ranking = True

    # If we have the site, inlet and instrument then just return the data
    # TODO - should instrument be added here
    if {"site", "inlet", "species"} <= search_kwargs.keys() or skip_ranking is True:
        specific_sources = aDict()
        for datasource in matching_sources.values():
            specific_keys = datasource.keys_in_daterange(start_date=start_date, end_date=end_date)

            if not specific_keys:
                continue

            metadata = datasource.metadata()

            site = metadata["site"]
            species = metadata["species"]
            inlet = metadata["inlet"]

            # Note that the keys here is just a list unlike the ranked keys dictionary
            # which contains the dateranges covered.
            specific_sources[site][species][inlet]["keys"]["unranked"] = specific_keys
            specific_sources[site][species][inlet]["metadata"] = metadata

        return SearchResults(results=specific_sources.to_dict(), ranked_data=False)

    highest_ranked = aDict()

    for uid, datasource in matching_sources.items():
        # Find the site and then the ranking
        metadata = datasource.metadata()
        # Get the site inlet and species
        site = metadata["site"]
        species = metadata["species"]

        rank_data = obj.get_rank(uuid=uid, start_date=start_date, end_date=end_date)

        # If this Datasource doesn't have any ranking data skip it and move on
        if not rank_data:
            continue

        # There will only be a single rank key
        rank_value = next(iter(rank_data))
        # Get the daterange this rank covers
        rank_dateranges = rank_data[rank_value]

        # Each match we store gives us the information we need
        # to retrieve the data
        match = {"uuid": uid, "dateranges": rank_dateranges}

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

    if not highest_ranked:
        raise ValueError(
            (
                "No ranking data set for the given search parameters."
                " Please refine your search to include a specific site, species and inlet."
            )
        )
    # Now we have the highest ranked data the dateranges there are ranks for
    # we want to fill in the gaps with (currently) the highest inlet from that site

    # We just want some rank_metadata to go along with the final data scheme
    # Can key a key of date - inlet
    data_keys: Dict = aDict()
    for site, species in highest_ranked.items():
        for sp, data in species.items():
            species_keys = {}
            species_rank_data = {}
            species_metadata = {}

            for match_data in data["matching"]:
                uuid = match_data["uuid"]
                match_dateranges = match_data["dateranges"]
                # Get the datasource as it's already in the dictionary
                # we created earlier
                datasource = matching_sources[uuid]
                metadata = datasource.metadata()
                inlet = metadata["inlet"]

                for dr in match_dateranges:
                    keys = datasource.keys_in_daterange_str(daterange=dr)

                    if keys:
                        # We'll add this to the metadata in the search results we return at the end
                        species_rank_data[dr] = inlet
                        species_keys[dr] = keys

                species_metadata[inlet] = metadata

            # Only create the dictionary keys if we have some data keys
            if species_keys:
                data_keys[site][sp]["keys"]["ranked"] = species_keys
                data_keys[site][sp]["rank_metadata"] = species_rank_data
                data_keys[site][sp]["metadata"] = species_metadata
            else:
                continue

            # We now need to retrieve data for the dateranges for which we don't have ranking data
            # To do this find the gaps in the daterange over which the user has requested data
            # and the dates for which we have ranking information

            # Get the dateranges that are covered by ranking information
            daterange_strs = list(iter_chain.from_iterable([m["dateranges"] for m in data["matching"]]))
            # # Find the gaps in the ranking coverage
            gap_dateranges = find_daterange_gaps(
                start_search=start_date, end_search=end_date, dateranges=daterange_strs
            )

            def max_key(s: str) -> float:
                return float(s.rstrip("m"))

            # Here just select the highest inlet that's been ranked and use that
            highest_inlet = max(sorted(list(data_keys[site][sp]["metadata"].keys())), key=max_key)

            inlet_metadata = data_keys[site][sp]["metadata"][highest_inlet]
            inlet_instrument = inlet_metadata["instrument"]
            inlet_sampling_period = inlet_metadata["sampling_period"]

            unranked_keys = []
            for gap_daterange in gap_dateranges:
                gap_start, gap_end = split_daterange_str(gap_daterange)

                results: SearchResults = search(
                    site=site,
                    species=sp,
                    inlet=highest_inlet,
                    instrument=inlet_instrument,
                    sampling_period=inlet_sampling_period,
                    start_date=gap_start,
                    end_date=gap_end,
                )  # type: ignore

                if not results:
                    continue

                # Retrieve the data keys
                inlet_data_keys = results.keys(site=site, species=sp, inlet=highest_inlet)["unranked"]

                unranked_keys.extend(inlet_data_keys)

            # If we've got keys that overlap two dateranges we'll get duplicates, remove those here
            data_keys[site][sp]["keys"]["unranked"] = list(set(unranked_keys))

    # TODO - create a stub for addict
    dict_data_keys = data_keys.to_dict()  # type: ignore

    return SearchResults(results=dict_data_keys, ranked_data=True)


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
#     from openghg.modules import Datasource, Footprints

#     if not isinstance(sites, list):
#         sites = [sites]

#     if not isinstance(domains, list):
#         domains = [domains]

#     footprints = Footprints.load()
#     datasource_uuids = footprints.datasources()
#     datasources = (Datasource.load(uuid=uuid, shallow=True) for uuid in datasource_uuids)

#     keys = defaultdict(dict)
#     # If we have locations to search
#     # for sites in sites:
#     for datasource in datasources:
#         for site in sites:
#             # TODO - should we iterate over the domains? Will there be the same site in multiple footprints domains?
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
