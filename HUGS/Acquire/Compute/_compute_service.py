
import uuid as _uuid
from copy import copy as _copy

from Acquire.Service import Service as _Service

__all__ = ["ComputeService"]


class ComputeService(_Service):
    """This is a specialisation of Service for Compute Services"""
    def __init__(self, other=None):
        if isinstance(other, _Service):
            self.__dict__ = _copy(other.__dict__)

            if not self.is_compute_service():
                from Acquire.Compute import ComputeServiceError
                raise ComputeServiceError(
                    "Cannot construct a ComputeService from "
                    "a service which is not an compute service!")
        else:
            _Service.__init__(self)

    def _call_local_function(self, function, args):
        """Internal function called to short-cut local 'remote'
           function calls
        """
        from compute.route import compute_functions as _compute_functions
        from admin.handler import create_handler as _create_handler
        handler = _create_handler(_compute_functions)
        return handler(function=function, args=args)
