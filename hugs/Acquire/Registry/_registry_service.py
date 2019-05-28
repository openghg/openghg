
from Acquire.Service import Service as _Service

__all__ = ["RegistryService"]


class RegistryService(_Service):
    """This is a specialisation of Service for Registry Services"""
    def __init__(self, other=None):
        if isinstance(other, _Service):
            from copy import copy as _copy
            self.__dict__ = _copy(other.__dict__)

            if not self.is_registry_service():
                from Acquire.Registry import RegistryServiceError
                raise RegistryServiceError(
                    "Cannot construct a RegistryService from "
                    "a service which is not a registry service!")
        else:
            _Service.__init__(self)

    def _call_local_function(self, function, args):
        """Internal function called to short-cut local 'remote'
           function calls
        """
        from registry.route import registry_functions as _registry_functions
        from admin.handler import create_handler as _create_handler
        handler = _create_handler(_registry_functions)
        return handler(function=function, args=args)

    def get_service(self, service_url=None, service_uid=None):
        """Ask the registry to return the service with specified
           service_url or service_uid
        """
        if service_url is None and service_uid is None:
            raise PermissionError(
                "You must specify one of service_url or service_uid")

        args = {"service_uid": service_uid,
                "service_url": service_url}

        response = self.call_function(function="get_service",
                                      args=args)

        from Acquire.Service import Service as _Service
        service = _Service.from_data(response["service_data"])

        return service
