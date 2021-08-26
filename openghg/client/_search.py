from typing import Dict, List, Optional, Union
from openghg.dataobjects import SearchResults
from Acquire.Client import Wallet


__all__ = ["Search"]


class Search:
    def __init__(self, service_url: Optional[str] = None):
        if service_url is not None:
            self._service_url = service_url
        else:
            self._service_url = "https://fn.openghg.org/t"

        wallet = Wallet()
        self._service = wallet.get_service(service_url=f"{self._service_url}/openghg")

    def search(
        self,
        species: Union[str, List] = None,
        site: Union[str, List] = None,
        inlet: Union[str, List] = None,
        instrument: Union[str, List] = None,
        start_date: str = None,
        end_date: str = None,
        skip_ranking: bool = False,
        data_type: str = "timeseries",
    ) -> Union[SearchResults, Dict]:
        """Search for surface observations data in the object store

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
        if self._service is None:
            raise PermissionError("Cannot use a null service")

        if not any((species, site, inlet, instrument)):
            raise ValueError("We must have at least one of  species, site, inlet or instrument")

        args = {}

        if species is not None:
            args["species"] = species

        if site is not None:
            args["site"] = site

        if inlet is not None:
            args["inlet"] = inlet

        if instrument is not None:
            args["instrument"] = instrument

        if start_date is not None:
            args["start_date"] = start_date
        if end_date is not None:
            args["end_date"] = end_date

        args["skip_ranking"] = str(skip_ranking)
        args["data_type"] = str(data_type)

        response: Dict = self._service.call_function(function="search.search", args=args)

        try:
            results_data = response["results"]
            search_results = SearchResults.from_data(results_data)
            return search_results
        except KeyError:
            return response
