from __future__ import annotations
from typing import TYPE_CHECKING, Dict, List, Union

# from Acquire.Client import Wallet
from openghg.retrieve import search as _local_search
from openghg.util import running_in_cloud

if TYPE_CHECKING:
    from openghg.dataobjects import SearchResults


def search(
    species: str = None,
    site: str = None,
    inlet: str = None,
    instrument: str = None,
    start_date: str = None,
    end_date: str = None,
) -> Union[SearchResults, Dict]:
    """Cloud object store search

    Args:
        species: Species
        site: Three letter site code
        inlet: Inlet height
        instrument: Instrument name
        start_date: Start date
        end_date: End date
    Returns:
        SearchResults:  SearchResults object
    """
    cloud = running_in_cloud()

    if cloud:
        raise NotImplementedError
        # return _cloud_search(
        #     species=species,
        #     site=site,
        #     inlet=inlet,
        #     instrument=instrument,
        #     start_date=start_date,
        #     end_date=end_date,
        # )
    else:
        results: Union[Dict, SearchResults] = _local_search(
            species=species,
            site=site,
            inlet=inlet,
            instrument=instrument,
            start_date=start_date,
            end_date=end_date,
        )

        return results


def _cloud_search(
    species: Union[str, List] = None,
    site: Union[str, List] = None,
    inlet: Union[str, List] = None,
    instrument: Union[str, List] = None,
    start_date: str = None,
    end_date: str = None,
    skip_ranking: bool = False,
    data_type: str = "timeseries",
    service_url: str = "https://fn.openghg.org/t",
) -> Union[SearchResults, Dict]:
    """Cloud object store search

    Args:
        species: Species
        site: Three letter site code
        inlet: Inlet height
        instrument: Instrument name
        start_date: Start date
        end_date: End date
    Returns:
        SearchResults:  SearchResults object
    """
    raise NotImplementedError

    # from openghg.dataobjects import SearchResults

    # wallet = Wallet()
    # cloud_service = wallet.get_service(service_url=f"{service_url}/openghg")

    # if not any((species, site, inlet, instrument)):
    #     raise ValueError("We must have at least one of  species, site, inlet or instrument")

    # args = {}

    # if species is not None:
    #     args["species"] = species

    # if site is not None:
    #     args["site"] = site

    # if inlet is not None:
    #     args["inlet"] = inlet

    # if instrument is not None:
    #     args["instrument"] = instrument

    # if start_date is not None:
    #     args["start_date"] = start_date
    # if end_date is not None:
    #     args["end_date"] = end_date

    # args["skip_ranking"] = str(skip_ranking)
    # args["data_type"] = str(data_type)

    # response: Dict = cloud_service.call_function(function="search.search", args=args)

    # try:
    #     results_data = response["results"]
    #     search_results = SearchResults.from_data(results_data)
    #     return search_results
    # except KeyError:
    #     return response
