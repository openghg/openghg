
__all__ = ["Hello"]


class Hello:
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

    def send_hello(self, name=None):
        if self._service is None:
            raise PermissionError("Cannot say hello to a null service!")

        if name is None:
            name = "World"

        args = {"name": name}

        response = self._service.call_function(function="hello", 
                                               args=args)

        return response["greeting"]

    def service(self):
        return self._service

