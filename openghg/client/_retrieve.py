from typing import Dict, List, Optional, Union

from Acquire.Client import Wallet

__all__ = ["Retrieve"]


class Retrieve:
    """
    This class is used to retrieve the data that's found using the search function
    from the object store
    """

    def __init__(self, service_url: Optional[str] = None):
        if service_url is not None:
            self._service_url = service_url
        else:
            self._service_url = "https://fn.openghg.org/t"

        wallet = Wallet()
        self._service = wallet.get_service(service_url=f"{self._service_url}/openghg")

    def retrieve(self, keys: Union[str, List]) -> Dict:
        """Retrieve the data at the keys found by the search function

        Args:
            keys: List of object store keys
        Returns:
            dict: Dictionary of xarray Datasets
        """
        from Acquire.ObjectStore import string_to_datetime
        from xarray import Dataset
        from json import loads as json_loads
        import warnings

        if self._service is None:
            raise PermissionError("Cannot use a null service")

        args = {}
        args["keys"] = keys
        args["return_type"] = "binary"

        response = self._service.call_function(function="retrieve.retrieve", args=args)

        response_data = response["results"]

        # Convert the string passed to dict
        for key in response_data:
            response_data[key] = json_loads(response_data[key])

        datasets = {}
        for key in response_data:
            # We need to convert the datetime string back to datetime objects here
            datetime_data = response_data[key]["coords"]["time"]["data"]

            for i, _ in enumerate(datetime_data):
                datetime_data[i] = string_to_datetime(datetime_data[i])

            # TODO - catch FutureWarnings here that may affect voila behaviour
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")

                json_data = response_data[key]
                datasets[key] = Dataset.from_dict(json_data)

        return datasets

    def service(self):
        return self._service
