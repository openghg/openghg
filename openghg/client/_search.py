__all__ = ["Search"]

import json
import warnings
import xarray
from Acquire.Client import Wallet
from Acquire.ObjectStore import string_to_datetime, datetime_to_string


class Search:
    def __init__(self, service_url=None):
        if service_url:
            self._service_url = service_url
        else:
            self._service_url = "https://openghg.acquire-aaai.com/t"

        wallet = Wallet()
        self._service = wallet.get_service(service_url=f"{self._service_url}/hugs")

    def search(self, locations, species=None, inlet=None, instrument=None, start_datetime=None, end_datetime=None):
        if self._service is None:
            raise PermissionError("Cannot use a null service")

        args = {}
        args["species"] = species
        args["locations"] = locations

        if inlet is not None:
            args["inlet"] = inlet

        if instrument is not None:
            args["instrument"] = instrument

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
        """ Downloads the selected keys and returns a dictionary of
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
