from typing import Dict, Optional
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

    def retrieve(self, keys: Dict) -> Dict:
        """Retrieve the data at the keys found by the search function

        Args:
            keys: Dictionary of object store keys, site_key: [keys]
        Returns:
            dict: Dictionary of xarray Datasets
        """
        from xarray import open_dataset

        if not isinstance(keys, dict):
            raise TypeError("keys must be a dictionary")

        if self._service is None:
            raise PermissionError("Cannot use a null service")

        args = {"keys": keys}

        response = self._service.call_function(function="retrieve.retrieve", args=args)

        response_data = response["results"]

        datasets = {key: open_dataset(data) for key, data in response_data.items()}

        return datasets
