__all__ = ["ClearDatasources"]


class ClearDatasources:
    def __init__(self, service_url=None):
        from Acquire.Client import Wallet as _Wallet
        wallet = _Wallet()
        self._service = wallet.get_service(service_url=service_url)

    def clear_datasources(self):
        if self._service is None:
            raise PermissionError("Cannot use a null service")

        args = {}

        self._service.call_function(function="clear_datasources", args=args)

    def service(self):
        return self._service
