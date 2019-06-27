__all__ = ["Goodbye"]

class Goodbye:
    """ How to do serverless

    """
    def __init__(self, service_url=None):
        if service_url is not None:
            from Acquire.Client import Wallet as _Wallet
            wallet = _Wallet()
            self._service = wallet.get_service(service_url=service_url)
        else:
            self._service = None
        
    def send_goodbye(self, name=None):
        if self._service is None:
            raise PermissionError("Cannot say goodbye to a null service")
        
        if name is None:
            name = "John"

        args = {"name": name}

        response = self._service.call_function(function="goodbye", args=args)

        return response["greeting"]

    def service(self):
        return self._service
