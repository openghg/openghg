from __future__ import annotations
from typing import Dict, Optional, Union, Any
from openghg.retrieve import search as _local_search
from openghg.util import running_in_cloud
from gzip import decompress
from openghg.dataobjects import SearchResults

# if TYPE_CHECKING:
#     from openghg.dataobjects import SearchResults


def search_surface(
    species: Optional[str] = None,
    site: Optional[str] = None,
    inlet: Optional[str] = None,
    instrument: Optional[str] = None,
    measurement_type: Optional[str] = None,
    data_type: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    **kwargs: Any,
) -> Union[SearchResults, Dict]:
    """Cloud object store search

    Args:
        species: Species
        site: Three letter site code
        inlet: Inlet height
        instrument: Instrument name
        measurement_type: Measurement type
        data_type: Data type e.g. CRDS, GCWERKS, ICOS
        start_date: Start date
        end_date: End date
        kwargs: Any other search arguments to constrain the search further
    Returns:
        SearchResults:  SearchResults object
    """

    if start_date is not None:
        start_date = str(start_date)
    if end_date is not None:
        end_date = str(end_date)

    results: Union[Dict, SearchResults] = search(
        species=species,
        site=site,
        inlet=inlet,
        instrument=instrument,
        measurement_type=measurement_type,
        data_type=data_type,
        start_date=start_date,
        end_date=end_date,
        **kwargs,
    )

    return results


# def search_emissions():
#     raise NotImplementedError


# def search_footprints():
#     raise NotImplementedError


# def search_met():
#     raise NotImplementedError


# def search_eulerian():
#     raise NotImplementedError


# def search_bc():
#     raise NotImplementedError


def search(**kwargs: Any) -> SearchResults:
    """This function accepts any keyword arguments and passes them directly to the internal search function
    Unless you know exactly which terms to use we suggest using one of the search helper functions.

    Args:
        kwargs: Any number of keyword arguments
    Returns:
        SearchResults: SearchResults object
    """
    from openghg.cloud import call_function

    cloud = running_in_cloud()

    if cloud:
        post_data: Dict[str, Union[str, Dict]] = {}
        post_data["function"] = "search"
        post_data["data"] = kwargs

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
        sr = _local_search(**kwargs)

    return sr
