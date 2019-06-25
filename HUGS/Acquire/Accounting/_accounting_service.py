
import uuid as _uuid
from copy import copy as _copy

from Acquire.Service import Service as _Service

__all__ = ["AccountingService"]


class AccountingService(_Service):
    """This is a specialisation of Service for Accounting Services"""
    def __init__(self, other=None):
        if isinstance(other, _Service):
            self.__dict__ = _copy(other.__dict__)

            if not self.is_accounting_service():
                from Acquire.Accounting import AccountingServiceError
                raise AccountingServiceError(
                    "Cannot construct an AccountingService from "
                    "a service which is not an accounting service!")
        else:
            _Service.__init__(self)

    def _call_local_function(self, function, args):
        """Internal function called to short-cut local 'remote'
           function calls

           Args:
                function (function): Function to route
                args: Arguments to pass to routed function

            Returns:
                function: A handler function
        """
        from accounting.route import accounting_functions \
            as _accounting_functions
        from admin.handler import create_handler as _create_handler
        handler = _create_handler(_accounting_functions)
        return handler(function=function, args=args)
