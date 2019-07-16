__all__ = ["Process"]


class Process:
    """ Process a datafile at a given PAR

    """
    def __init__(self, service_url=None):
        if service_url:
            from Acquire.Client import Wallet as _Wallet
            wallet = _Wallet()
            self._service = wallet.get_service(service_url=service_url)
        else:
            self._service

    def process_file(self, auth, par, par_secret):
        """ Pass a PAR for the file to be processed to the processing function

            Args:
                par : JSON serialised PAR object

        """
        if self._service is None:
            raise PermissionError("Cannot use a null service")

        args = {"authorisation": auth.to_data(),
                "file_par": par.to_data(),
                "par_secret": par_secret}

        response = self._service.call_function(function="process", args=args)

        return response["results"]
