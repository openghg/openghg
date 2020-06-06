__all__ = ["Status"]


class Status:
    def __init__(self, service_url=None):
        from Acquire.Client import Wallet as _Wallet

        wallet = _Wallet()
        self._service = wallet.get_service(service_url="%s/hugs" % service_url)

    def status(self):
        """ Returns True if we can call a function on the backend otherwise False

            Returns:
                True if function successful, else False
        """
        if self._service is None:
            raise PermissionError("Cannot use a null service")

        return self._service.call_function(function="status")

    def service(self):
        return self._service
