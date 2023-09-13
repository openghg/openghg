""" Generic search functions that can be used to find data in
    the object store

"""
import logging
from typing import Any, Dict, List, Optional, Union
import warnings
from openghg.store import load_metastore
from openghg.store.spec import define_data_type_classes, define_data_types
from openghg.objectstore import get_readable_buckets
from openghg.util import decompress, running_on_hub
from openghg.types import ObjectStoreError
from tinydb.database import TinyDB
from openghg.dataobjects import SearchResults

logger = logging.getLogger("openghg.retrieve")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


def _find_and(x: Any, y: Any) -> Any:
    return x & y


def _find_or(x: Any, y: Any) -> Any:
    return x | y


def meta_search(search_terms: Dict, database: TinyDB) -> Dict:
    """Search a metadata database and return dictionary of the
    metadata for each Datasource keyed by their UUIDs.

    Args:
        search_terms: Keys we want to find
        database: The tinydb database for the storage object
    Returns:
        dict: Dictionary of metadata
    """
    from functools import reduce

    from openghg.util import timestamp_epoch, timestamp_now, timestamp_tzaware
    from pandas import Timedelta
    from tinydb import Query

    # Do this here otherwise we have to produce them for every datasource
    start_date = search_terms.get("start_date")
    end_date = search_terms.get("end_date")

    if start_date is None:
        start_date = timestamp_epoch()
    else:
        start_date = timestamp_tzaware(start_date) + Timedelta("1s")

    if end_date is None:
        end_date = timestamp_now()
    else:
        end_date = timestamp_tzaware(end_date) - Timedelta("1s")

    q = Query()

    search_attrs = [getattr(q, k) == v for k, v in search_terms.items()]
    result = database.search(reduce(_find_and, search_attrs))

    x = [s["uuid"] for s in result]

    # Add in a quick check to make sure we don't have dupes
    # TODO - remove this once a more thorough tests are added
    if len(x) != len(set(x)):
        error_msg = "Multiple results found with same UUID!"
        logger.error(error_msg)
        raise ValueError(error_msg)

    return {s["uuid"]: s for s in result}


def search_bc(
    species: Optional[str] = None,
    bc_input: Optional[str] = None,
    domain: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    period: Optional[Union[str, tuple]] = None,
    continuous: Optional[bool] = None,
    **kwargs: Any,
) -> SearchResults:
    """Search for boundary condition data

    Args:
        species: Species name
        bc_input: Input used to create boundary conditions. For example:
            - a model name such as "MOZART" or "CAMS"
            - a description such as "UniformAGAGE" (uniform values based on AGAGE average)
        domain: Region for boundary conditions
        start_date: Start date (inclusive) for boundary conditions
        end_date: End date (exclusive) for boundary conditions
        period: Period of measurements. Only needed if this can not be inferred from the time coords
                If specified, should be one of:
                    - "yearly", "monthly"
                    - suitable pandas Offset Alias
                    - tuple of (value, unit) as would be passed to pandas.Timedelta function
        continuous: Whether time stamps have to be continuous.
        kwargs: Additional search terms
    Returns:
        SearchResults: SearchResults object
    """

    if start_date is not None:
        start_date = str(start_date)
    if end_date is not None:
        end_date = str(end_date)

    return search(
        species=species,
        bc_input=bc_input,
        domain=domain,
        start_date=start_date,
        end_date=end_date,
        period=period,
        continuous=continuous,
        data_type="boundary_conditions",
        **kwargs,
    )


def search_eulerian(
    model: Optional[str] = None,
    species: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    **kwargs: Any,
) -> SearchResults:
    """Search for eulerian data

    Args:
        model: Eulerian model name
        species: Species name
        start_date: Start date (inclusive) associated with model run
        end_date: End date (exclusive) associated with model run
        kwargs: Additional search terms
    Returns:
        SearchResults: SearchResults object
    """

    if start_date is not None:
        start_date = str(start_date)
    if end_date is not None:
        end_date = str(end_date)

    return search(
        model=model,
        species=species,
        start_date=start_date,
        end_date=end_date,
        data_type="eulerian_model",
        **kwargs,
    )


def search_flux(
    species: Optional[str] = None,
    source: Optional[str] = None,
    domain: Optional[str] = None,
    database: Optional[str] = None,
    database_version: Optional[str] = None,
    model: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    time_resolved: Optional[bool] = None,
    high_time_resolution: Optional[bool] = None,  # DEPRECATED: use time_resolved instead
    period: Optional[Union[str, tuple]] = None,
    continuous: Optional[bool] = None,
    **kwargs: Any,
) -> SearchResults:
    """Search for emissions data

    Args:
        species: Species name
        domain: Emissions domain
        source: Emissions source
        database: Name of database source for this input (if relevant)
        database_version: Name of database version (if relevant)
        model: Model name (if relevant)
        source_format : Type of data being input e.g. openghg (internal format)
        time_resolved: If this is a high resolution file
        high_time_resolution: This argument is deprecated and will be replaced in future versions with time_resolved.
        period: Period of measurements. Only needed if this can not be inferred from the time coords
                If specified, should be one of:
                    - "yearly", "monthly"
                    - suitable pandas Offset Alias
                    - tuple of (value, unit) as would be passed to pandas.Timedelta function
        continuous: Whether time stamps have to be continuous.
        kwargs: Additional search terms
    Returns:
        SearchResults: SearchResults object
    """

    # `high_time_resolution` is checked and stored in `time_resolved` with deprecation warning
    if high_time_resolution is not None:
        warnings.warn("This argument is deprecated and will be replaced in future versions with time_resolved.", DeprecationWarning)
        time_resolved = high_time_resolution
    if start_date is not None:
        start_date = str(start_date)
    if end_date is not None:
        end_date = str(end_date)

    return search(
        species=species,
        source=source,
        domain=domain,
        database=database,
        database_version=database_version,
        model=model,
        start_date=start_date,
        end_date=end_date,
        time_resolved=time_resolved,
        period=period,
        continuous=continuous,
        data_type="emissions",
        **kwargs,
    )


def search_footprints(
    site: Optional[str] = None,
    inlet: Optional[str] = None,
    domain: Optional[str] = None,
    model: Optional[str] = None,
    height: Optional[str] = None,
    metmodel: Optional[str] = None,
    species: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    network: Optional[str] = None,
    period: Optional[Union[str, tuple]] = None,
    continuous: Optional[bool] = None,
    high_spatial_resolution: Optional[bool] = None,  # TODO need to give False to get only low spatial res
    time_resolved: Optional[bool] = None,
    high_time_resolution: Optional[bool] = None,
    short_lifetime: Optional[bool] = None,
    **kwargs: Any,
) -> SearchResults:
    """Search for footprints data

    Args:
        site: Site name
        inlet: Height above ground level in metres
        domain: Domain of footprints
        model: Model used to create footprint (e.g. NAME or FLEXPART)
        height: Alias for inlet
        metmodel: Underlying meteorlogical model used (e.g. UKV)
        species: Species name. Only needed if footprint is for a specific species e.g. co2 (and not inert)
        network: Network name
        period: Period of measurements. Only needed if this can not be inferred from the time coords
        continuous: Whether time stamps have to be continuous.
        retrieve_met: Whether to also download meterological data for this footprints area
        high_spatial_resolution : Indicate footprints include both a low and high spatial resolution.
        time_resolved: Indicate footprints are high time resolution (include H_back dimension)
                        Note this will be set to True automatically if species="co2" (Carbon Dioxide).
        high_time_resolution: This argument is deprecated and will be replaced in future versions with time_resolved.
        short_lifetime: Indicate footprint is for a short-lived species. Needs species input.
                        Note this will be set to True if species has an associated lifetime.
        kwargs: Additional search terms
    Returns:
        SearchResults: SearchResults object
    """
    from openghg.util import format_inlet
    # `high_time_resolution` is checked and stored in `time_resolved` with deprecation warning
    if high_time_resolution is not None:
        warnings.warn("This feature is deprecated and will be replaced in future versions with time_resolved.", DeprecationWarning)
        time_resolved = high_time_resolution
    args = {
        "site": site,
        "inlet": inlet,
        "height": height,
        "domain": domain,
        "model": model,
        "metmodel": metmodel,
        "species": species,
        "network": network,
        "start_date": start_date,
        "end_date": end_date,
        "period": period,
        "continuous": continuous,
        "time_resolved": time_resolved,
        "high_spatial_resolution": high_spatial_resolution,
        "short_lifetime": short_lifetime,
    }

    # Keys in metastore are stored as strings; convert non-string arguments to strings.
    for k in ["start_date", "end_date"]:
        if args[k] is not None:
            args[k] = str(args[k])

    # Either (or both) of 'inlet' and 'height' may be in the metastore, so
    # both are allowed for search.
    args["inlet"] = format_inlet(inlet)
    args["height"] = format_inlet(height)

    args["data_type"] = "footprints"  # generic `search` needs the data type

    # merge kwargs and args, keeping values from args on key conflict
    kwargs.update(args)

    return search(**kwargs)


def search_surface(
    species: Union[str, List[str], None] = None,
    site: Union[str, List[str], None] = None,
    inlet: Union[str, List[str], None] = None,
    height: Union[str, List[str], None] = None,
    instrument: Union[str, List[str], None] = None,
    measurement_type: Union[str, List[str], None] = None,
    source_format: Union[str, List[str], None] = None,
    network: Union[str, List[str], None] = None,
    start_date: Union[str, List[str], None] = None,
    end_date: Union[str, List[str], None] = None,
    data_source: Optional[str] = None,
    sampling_height: Optional[str] = None,
    icos_data_level: Optional[int] = None,
    dataset_source: Optional[str] = None,
    **kwargs: Any,
) -> SearchResults:
    """Cloud object store search

    Args:
        species: Species
        site: Three letter site code
        inlet: Inlet height above ground level in metres
        height: Alias for inlet
        instrument: Instrument name
        measurement_type: Measurement type
        data_type: Data type e.g. "surface", "column", "emissions"
            See openghg.store.spec.define_data_types() for full details.
        start_date: Start date
        end_date: End date
        data_source: Source of data, e.g. noaa_obspack, icoscp, ceda_archive. This
        argument only needs to be used to narrow the search to data solely from these sources.
        sampling_height: Sampling height of measurements
        icos_data_level: ICOS data level, see ICOS documentation
        dataset_source: For ICOS data only: dataset source name, for example ICOS, InGOS, European ObsPack
        kwargs: Additional search terms
    Returns:
        SearchResults: SearchResults object
    """
    from openghg.util import format_inlet

    if start_date is not None:
        start_date = str(start_date)
    if end_date is not None:
        end_date = str(end_date)

    # Allow height to be an alias for inlet but we do not expect height
    # to be within the metadata (for now)
    if inlet is None and height is not None:
        inlet = height
    if isinstance(inlet, list):
        inlet = [format_inlet(value) for value in inlet]
    else:
        inlet = format_inlet(inlet)

    results = search(
        species=species,
        site=site,
        inlet=inlet,
        instrument=instrument,
        measurement_type=measurement_type,
        data_type="surface",
        source_format=source_format,
        start_date=start_date,
        end_date=end_date,
        data_source=data_source,
        network=network,
        sampling_height=sampling_height,
        icos_data_level=icos_data_level,
        dataset_source=dataset_source,
        **kwargs,
    )

    return results


def search_column(
    satellite: Optional[str] = None,
    domain: Optional[str] = None,
    selection: Optional[str] = None,
    site: Optional[str] = None,
    species: Optional[str] = None,
    network: Optional[str] = None,
    instrument: Optional[str] = None,
    platform: Optional[str] = None,
    **kwargs: Any,
) -> SearchResults:
    """Search column data

    Args:
        satellite: Name of satellite (if relevant)
        domain: For satellite only. If data has been selected on an area include the
            identifier name for domain covered. This can map to previously defined domains
            (see openghg_defs "domain_info.json" file) or a newly defined domain.
        selection: For satellite only, identifier for any data selection which has been
            performed on satellite data. This can be based on any form of filtering, binning etc.
            but should be unique compared to other selections made e.g. "land", "glint", "upperlimit".
            If not specified, domain will be used.
        site : Site code/name (if relevant). Can include satellite OR site.
        species: Species name or synonym e.g. "ch4"
        instrument: Instrument name e.g. "TANSO-FTS"
        network: Name of in-situ or satellite network e.g. "TCCON", "GOSAT"
        platform: Type of platform. Should be one of:
            - "satellite"
            - "site"
        kwargs: Additional search terms
    Returns:
        SearchResults: SearchResults object
    """
    return search(
        satellite=satellite,
        domain=domain,
        selection=selection,
        site=site,
        species=species,
        network=network,
        instrument=instrument,
        platform=platform,
        data_type="column",
        **kwargs,
    )


def search(**kwargs: Any) -> SearchResults:
    """Search for observations data. Any keyword arguments may be passed to the
    the function and these keywords will be used to search the metadata associated
    with each Datasource.

    This function detects the running environment and routes the call
    to either the cloud or local search function.

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
    Returns:
        SearchResults or None: SearchResults object is results found, otherwise None
    """
    from openghg.cloud import call_function

    if running_on_hub():
        post_data: Dict[str, Union[str, Dict]] = {}
        post_data["function"] = "search"
        post_data["search_terms"] = kwargs

        result = call_function(data=post_data)

        content = result["content"]

        found = content["found"]
        compressed_response = content["result"]

        if found:
            data_str = decompress(compressed_response)
            sr = SearchResults.from_json(data=data_str)
        else:
            sr = SearchResults()
    else:
        sr = _base_search(**kwargs)

    return sr


def _base_search(**kwargs: Any) -> SearchResults:
    """Search for observations data. Any keyword arguments may be passed to the
    the function and these keywords will be used to search metadata.

    This function will only perform a "local" search. It may be used either by a cloud function
    or when using OpenGHG locally, it does no environment detection.
    We suggest using the search function that takes care of everything for you.

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
    Returns:
        SearchResults or None: SearchResults object is results found, otherwise None
    """
    import itertools
    from openghg.store.base import Datasource
    from openghg.dataobjects import SearchResults
    from openghg.util import (
        clean_string,
        synonyms,
        timestamp_epoch,
        timestamp_now,
        timestamp_tzaware,
    )
    from pandas import Timedelta as pd_Timedelta
    from tinydb import Query

    if running_on_hub():
        raise ValueError(
            "This function can't be used on the OpenGHG Hub, please use openghg.retrieve.search instead."
        )

    # As we might have kwargs that are None we want to get rid of those
    search_kwargs = {k: clean_string(v) for k, v in kwargs.items() if v is not None}

    # Species translation
    species = search_kwargs.get("species")

    if species is not None:
        if not isinstance(species, list):
            species = [species]

        updated_species = [synonyms(sp) for sp in species]
        search_kwargs["species"] = updated_species

    # translate data type strings to data type classes
    data_type = search_kwargs.get("data_type")
    data_type_classes = define_data_type_classes()

    types_to_search = []
    if data_type is not None:
        if not isinstance(data_type, list):
            data_type = [data_type]

        valid_data_types = define_data_types()
        for d in data_type:
            if d not in valid_data_types:
                raise ValueError(
                    f"{data_type} is not a valid data type, please select one of {valid_data_types}"
                )
            # Get the object we want to load in from the object store
            type_class = data_type_classes[d]
            types_to_search.append(type_class)
    else:
        types_to_search.extend(data_type_classes.values())

    # Get a dictionary of all the readable buckets available
    # We'll iterate over each of them
    readable_buckets = get_readable_buckets()

    # If we're given a store then we'll just read from that one
    try:
        store = search_kwargs["store"]
    except KeyError:
        pass
    else:
        del search_kwargs["store"]

        try:
            readable_buckets = {store: readable_buckets[store]}
        except KeyError:
            raise ObjectStoreError(f"Invalid store: {store}")

    try:
        start_date = search_kwargs["start_date"]
    except KeyError:
        start_date = None
    else:
        del search_kwargs["start_date"]

    try:
        end_date = search_kwargs["end_date"]
    except KeyError:
        end_date = None
    else:
        del search_kwargs["end_date"]

    # Here we process the kwargs and flatten out the lists so
    # we create the combinations of search queries correctly
    a_list = {}
    not_a_list = {}
    for k, v in search_kwargs.items():
        if isinstance(v, (list, tuple)):
            a_list[k] = v
        else:
            not_a_list[k] = v

    # If we have lists of values to find we need to flatten them out
    expanded_search = []
    if a_list:
        keys, values = zip(*a_list.items())
        for v in itertools.product(*values):
            d = dict(zip(keys, v))
            if not_a_list:
                d.update(not_a_list)
            expanded_search.append(d)
    else:
        expanded_search.append(not_a_list)

    data_keys = {}
    general_metadata = {}

    for bucket_name, bucket in readable_buckets.items():
        metastore_records = []
        for data_type_class in types_to_search:
            metakey = data_type_class._metakey

            with load_metastore(bucket=bucket, key=metakey, mode="r") as metastore:
                for v in expanded_search:
                    res = metastore.search(Query().fragment(v))
                    if res:
                        metastore_records.extend(res)

        if not metastore_records:
            continue

        # Add in a quick check to make sure we don't have dupes
        # TODO - remove this once a more thorough tests are added
        uuids = [s["uuid"] for s in metastore_records]
        if len(uuids) != len(set(uuids)):
            error_msg = "Multiple results found with same UUID!"
            logger.exception(msg=error_msg)
            raise ValueError(error_msg)

        # Here we create a dictionary of the metadata keyed by the Datasource UUID
        # we'll create a pandas DataFrame out of this in the SearchResult object
        # for better printing / searching within a notebook
        metadata = {r["uuid"]: r for r in metastore_records}
        # Add in the object store to the metadata the user sees
        for m in metadata.values():
            m.update({"object_store": bucket})

        # Narrow the search to a daterange if dates passed
        if start_date is not None or end_date is not None:
            if start_date is None:
                start_date = timestamp_epoch()
            else:
                start_date = timestamp_tzaware(start_date) + pd_Timedelta("1s")

            if end_date is None:
                end_date = timestamp_now()
            else:
                end_date = timestamp_tzaware(end_date) - pd_Timedelta("1s")

            metadata_in_daterange = {}

            # TODO - we can remove this now the metastore contains the start and end dates of the Datasources
            for uid, record in metadata.items():
                _keys = Datasource.load(bucket=bucket, uuid=uid, shallow=True).keys_in_daterange(
                    start_date=start_date, end_date=end_date
                )

                if _keys:
                    metadata_in_daterange[uid] = record
                    data_keys[uid] = _keys

            if not data_keys:
                logger.warning(
                    f"No data found for the dates given in the {bucket_name} store, please try a wider search."
                )
            # Update the metadata we'll use to create the SearchResults object
            metadata = metadata_in_daterange
        else:
            for uid in metadata:
                data_keys[uid] = Datasource.load(bucket=bucket, uuid=uid, shallow=True).data_keys()

        # TODO - adding this to make sure everything is working correctly
        # Remove once more comprehensive tests are done
        dupe_uuids = [k for k in metadata if k in general_metadata]
        if dupe_uuids:
            raise ObjectStoreError("Duplicate UUIDs found between buckets.")

        general_metadata.update(metadata)

    return SearchResults(keys=dict(data_keys), metadata=dict(general_metadata), start_result="data_type")
