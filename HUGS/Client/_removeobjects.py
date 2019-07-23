# Used to clear the bucket for testing and searching purposes

__all__ = ["RemoveObjects"]


class RemoveObjects:
    def __init__(self, service_url=None):
        if service_url is not None:
            from Acquire.Client import Wallet as _Wallet
            wallet = _Wallet()
            self._service = wallet.get_service(service_url=service_url)
        else:
            self._service = None

    def remove_objects(self, keys):
        """ Delete the objects at keys in the bucket

            Args:
                keys (list): List of keys to delete
            Returns:
                response (dict): Response of clear_bucket function
        """
        if self._service is None:
            raise PermissionError("Cannot use a null service")

        args = {"keys": keys}

        response = self._service.call_function(function="remove_objects", args=args)

        return response["results"]

    def service(self):
        return self._service
