__all__ = ["Search"]


class Search:
    def __init__(self, service_url=None):
        from Acquire.Client import Wallet as _Wallet
        wallet = _Wallet()
        self._service = wallet.get_service(service_url="%s/hugs" % service_url)
        
    def search(self, search_terms, locations, data_type, start_datetime=None, end_datetime=None):
        if self._service is None:
            raise PermissionError("Cannot use a null service")

        from Acquire.ObjectStore import datetime_to_string as _datetime_to_string

        args = {}
        args["search_terms"] = search_terms
        args["locations"] = locations
        args["data_type"] = data_type

        if start_datetime:
            args["start_datetime"] = _datetime_to_string(start_datetime)
        if end_datetime:
            args["end_datetime"] = _datetime_to_string(end_datetime)

        response = self._service.call_function(function="search", args=args)

        return response["results"]
    
    def service(self):
        return self._service

        