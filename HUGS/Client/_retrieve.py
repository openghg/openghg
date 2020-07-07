__all__ = ["Retrieve"]


class Retrieve:
    """
    This class is used to retrieve the data that's found using the search function
    from the object store
    """

    def __init__(self, service_url=None):
        from Acquire.Client import Wallet as _Wallet

        wallet = _Wallet()
        self._service = wallet.get_service(service_url="%s/hugs" % service_url)

    def list(self):
        """ Return details on the search results

            Returns:
                list: List of keys of search results
        """
        return list(self._results.keys())

    def retrieve(self, keys):
        """ Retrieve the data at the keys found by the search function

            Args:
                keys (dict): Dictionary of object store keys
            Returns:
                dict: Dictionary of xarray Datasets
        """
        from Acquire.ObjectStore import string_to_datetime
        from xarray import Dataset
        from json import loads
        import warnings

        if self._service is None:
            raise PermissionError("Cannot use a null service")

        args = {}
        args["keys"] = keys
        args["return_type"] = "json"

        response = self._service.call_function(function="retrieve", args=args)

        response_data = response["results"]

        # Convert the string passed to dict
        for key in response_data:
            response_data[key] = loads(response_data[key])

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
