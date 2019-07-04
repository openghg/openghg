__all__ = ["Search"]

class Search:
    def __init__(self, service_url=None):
        from Acquire.Client import Wallet as _Wallet
        wallet = _Wallet()
        self._service = wallet.get_service(service_url=service_url)
        

    def search(self, species, data_type):
        if self._service is None:
            raise PermissionError("Cannot use a null service")

        args = {"species" : species, "data_type" : data_type}

        response = self._service.call_function(function="search", args=args)

        return response["results"]
    
    def service(self):
        return self._service

        