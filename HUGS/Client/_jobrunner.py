__all__ = ["JobRunner"]

class JobRunner:
    def __init__(self, service_url=None):
        from Acquire.Client import Wallet
        
        wallet = Wallet()
        
        self._service = wallet.get_service(service_url="%s/hugs" % service_url)

    def run(self, job_name, job_type, data):
        if self._service is None:
            raise PermissionError("Cannot use a null service")

        args = {}
        args["job_name"] = job_name
        args["job_type"] = job_type
        args["data"] = data

        response = self._service.call_function(function="jobrunner", args=args)

        return response["results"]

    def service(self):
        return self._service