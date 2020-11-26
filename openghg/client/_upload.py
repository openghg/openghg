__all__ = ["Upload"]


class Upload:
    """This is a simple class to demonstrate client-server
       communication
    """

    def __init__(self, service_url=None):
        if service_url is not None:
            from Acquire.Client import Wallet as _Wallet

            wallet = _Wallet()
            self._service = wallet.get_service(service_url=service_url)
        else:
            self._service = None

    def upload(self, filename):
        if self._service is None:
            raise PermissionError("Cannot use a null service!")

        args = {"filename": filename}

        response = self._service.call_function(function="upload", args=args)

        return response["filemeta"]

    def service(self):
        return self._service
