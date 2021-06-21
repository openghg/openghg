import json
import warnings
from typing import Dict, List, Optional, Union
import xarray

from Acquire.Client import Wallet
from Acquire.ObjectStore import string_to_datetime, datetime_to_string


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
        species: Optional[Union[str, List]] = None,
        site: Optional[Union[str, List]] = None,
        inlet: Optional[Union[str, List]] = None,
        instrument: Optional[Union[str, List]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        skip_ranking: Optional[bool] = False,
        data_type: Optional[str] = "timeseries",
    ) -> Dict:
        """Search for surface observations data in the object store

        Args:
            species: Species
            site: Three letter site code
            inlet: Inlet height
            instrument: Instrument name
            start_date: Start date
            end_date: End date
        """
        from openghg.dataobjects import SearchResults

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
        args["data_type"] = data_type

        response = self._service.call_function(function="search.search", args=args)

        try:
            results_data = response["results"]
            search_results = SearchResults.from_data(results_data)
            return search_results
        except KeyError:
            return response

    def results(self):
        """Return the results in an easy to read format when printed to screen

        Returns:
            dict: Dictionary of results
        """
        return {
            key: f"Daterange : {self._results[key]['start_date']} - {self._results[key]['end_date']}" for key in self._results
        }

    def download(self, selected_keys):
        """Downloads the selected keys and returns a dictionary of
        xarray Datasets

        Args:
            keys (str, list): Key(s) from search results to download
        Returns:
            defaultdict(dict): Dictionary of Datasets
        """
        if not isinstance(selected_keys, list):
            selected_keys = [selected_keys]

        # Create a Retrieve object to interact with the object store
        # Select the keys we want to download
        download_keys = {key: self._results[key]["keys"] for key in selected_keys}

        args = {"keys": download_keys, "return_type": "json"}
        response = self._service.call_function(function="retrieve", args=args)
        result_data = response["results"]

        # datasets = defaultdict(dict)
        datasets = []
        # TODO - find a better way of doing this, returning compressed binary data would be far better
        for key, dateranges in result_data.items():
            for daterange in dateranges:
                serialised_data = json.loads(result_data[key][daterange])

                # We need to convert the datetime string back to datetime objects here
                datetime_data = serialised_data["coords"]["time"]["data"]
                for i, _ in enumerate(datetime_data):
                    datetime_data[i] = string_to_datetime(datetime_data[i])

                # TODO - catch FutureWarnings here that may affect run when used within voila
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    # datasets[key][daterange] = xarray.Dataset.from_dict(serialised_data)
                    datasets.append(xarray.Dataset.from_dict(serialised_data))

        return datasets

    def service(self):
        return self._service
