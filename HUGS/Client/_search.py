__all__ = ["Search"]

class Search:
    def __init__(self, service_url=None):
        if service_url is None:
            from Acquire.Client import Wallet as _Wallet
            wallet = _Wallet()
            self._service = wallet.get_service(service_url=service_url)
        else:
            self._service = None

    def search(self, species):
        if self._service is None:
            raise PermissionError("Cannot use a null service")

        args = {"species" : species}

        response = self._service.call_function(function="search", args=args)

        return response["results"]
    
    def service(self):
        return self._service

        