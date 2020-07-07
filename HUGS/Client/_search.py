__all__ = ["Search"]

import json
import warnings
import xarray
from Acquire.Client import Wallet
from Acquire.ObjectStore import string_to_datetime, datetime_to_string


class Search:
    def __init__(self, service_url=None):
        wallet = Wallet()
        self._service = wallet.get_service(service_url=f"{service_url}/hugs")
        self._results = {}

    def search(
        self, search_terms, locations, data_type, start_datetime=None, end_datetime=None
    ):
        if self._service is None:
            raise PermissionError("Cannot use a null service")

        args = {}
        args["search_terms"] = search_terms
        args["locations"] = locations
        args["data_type"] = data_type

        if start_datetime:
            args["start_datetime"] = datetime_to_string(start_datetime)
        if end_datetime:
            args["end_datetime"] = datetime_to_string(end_datetime)

        response = self._service.call_function(function="search", args=args)["results"]

        self._results = response

        return response

    def results(self):
        """ Return the results in an easy to read format when printed to screen

            Returns:    
                dict: Dictionary of results
        """
        return {
            key: f"Daterange : {self._results[key]['start_date']} - {self._results[key]['end_date']}"
            for key in self._results
        }

    def download(self, selected_keys):
        """ Download the selected keys

            TODO should this just return a single concatenated Dataset?

            Args:
                keys (str, list): Key(s) from search results to download
            Returns:
                dict: Dictionary of Datasets for 
        """
        if not isinstance(selected_keys, list):
            selected_keys = [selected_keys]

        # Create a Retrieve object to interact with the HUGS Cloud object store
        # Select the keys we want to download
        download_keys = {key: self._results[key]["keys"] for key in selected_keys}

        args = {"keys": download_keys, "return_type": "json"}
        response = self._service.call_function(function="retrieve", args=args)
        response_data = response["results"]

        # Convert the string passed to dict
        for key in response_data:
            response_data[key] = json.loads(response_data[key])

        datasets = {}
        # TODO - find a better way of doing this, returning binary data would be far better
        for key in response_data:
            # We need to convert the datetime string back to datetime objects here
            datetime_data = response_data[key]["coords"]["time"]["data"]

            for i, _ in enumerate(datetime_data):
                datetime_data[i] = string_to_datetime(datetime_data[i])

            # TODO - catch FutureWarnings here that may affect run when used within voila
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")

                json_data = response_data[key]
                datasets[key] = xarray.Dataset.from_dict(json_data)

        return datasets

    def service(self):
        return self._service
