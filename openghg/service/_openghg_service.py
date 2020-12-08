# from copy import copy as _copy

# from Acquire.Service import Service as _Service

# __all__ = ["HugsService"]


# class HugsService(_Service):
#     """This is a specialisation of Service for Hugs Services"""

#     def __init__(self, other=None):
#         if isinstance(other, _Service):
#             self.__dict__ = _copy(other.__dict__)

#             if self.service_type() != "hugs":
#                 from openghg.service import HugsServiceError

#                 raise HugsServiceError(
#                     "Cannot construct a HugsService from a service which is not a hugs service!"
#                 )
#         else:
#             _Service.__init__(self)

#     def _call_local_function(self, function, args):
#         """Internal function called to short-cut local 'remote'
#            function calls
#         """
#         from hugs_service.route import hugs_functions as _hugs_functions
#         from admin.handler import create_handler as _create_handler

#         handler = _create_handler(_hugs_functions)
#         return handler(function=function, args=args)
