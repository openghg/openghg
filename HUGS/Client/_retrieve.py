
__all__ = ["Retrieve"]


class Retrieve:
    """
    This class is used to retrieve the data that's found using the search function
    from the object store
    """
    def __init__(self, service_url=None):
        from Acquire.Client import Wallet as _Wallet
        wallet = _Wallet()
        self._service = wallet.get_service(service_url=service_url)

    def retrieve(self, keys):
        """ Retrieve the data at the keys found by the search function

            Args:
                keys (dict): Dictionary of object store keys
            Returns:
                dict: Dictionary of results at key results
        """
        if self._service is None:
            raise PermissionError("Cannot use a null service")

        args = {}
        args["keys"] = keys
        args["return_type"] = "json"

        response = self._service.call_function(function="retrieve", args=args)

        return response["results"]

    def service(self):
        return self._service
