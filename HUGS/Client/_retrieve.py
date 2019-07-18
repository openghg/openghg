"""
This class is used to retrieve the data that's found using the search function
from the object store
"""

__all__ = ["Retrieve"]

class Retrieve:
    def __init__(self, service_url=None):
        from Acquire.Client import Wallet as _Wallet
        wallet = _Wallet()
        self._service = wallet.get_service(service_url=service_url)

    def retrieve(self, keys):
        if self._service is None:
            raise PermissionError("Cannot use a null service")

        

