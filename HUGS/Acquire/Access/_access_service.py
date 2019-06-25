
import uuid as _uuid
from copy import copy as _copy

from Acquire.Service import Service as _Service

__all__ = ["AccessService"]


class AccessService(_Service):
    """This is a specialisation of Service for Access Services
       Where Service represents a service in the system. Services
       will either be identity services, access services,
       storage services or accounting services
    """

    def __init__(self, other=None):
        if isinstance(other, _Service):
            self.__dict__ = _copy(other.__dict__)

            if not self.is_access_service():
                from Acquire.Access import AccessServiceError
                raise AccessServiceError(
                    "Cannot construct an AccessService from "
                    "a service which is not an access service!")
        else:
            _Service.__init__(self)

    def _call_local_function(self, function, args):
        """Internal function called to short-cut local 'remote'
           function calls

           Args:
                function: function to route
                args: arguments to be passed into function

            Returns:
                function: function object
        """

        from access.route import access_functions as _access_functions
        from admin.handler import create_handler as _create_handler

        handler = _create_handler(_access_functions)
        return handler(function=function, args=args)

    def get_trusted_storage_service(self):
        """Return a trusted storage service

            Returns:
                :obj:`dict`: containing the first storage
                device on the trusted service

        """
        from Acquire.Service import get_trusted_services \
            as _get_trusted_services

        services = _get_trusted_services()

        try:
            return services["storage"][0]
        except:
            from Acquire.Service import ServiceError
            raise ServiceError(
                "There is no trusted storage service known to this access "
                "service")
