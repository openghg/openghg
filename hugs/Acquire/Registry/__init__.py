"""
Acquire : (C) Christopher Woods 2019

This module provides the classes and functions necessary to implement
an Acquire Registry Service. The Registry Service is responsible for
identifying and registering other services
"""

from ._get_registry_details import *
from ._get_trusted_registry import *
from ._register_service import *
from ._registry_service import *
from ._registry import *

try:
    if __IPYTHON__:
        def _set_printer(C):
            """Function to tell ipython to use __str__ if available"""
            get_ipython().display_formatter.formatters['text/plain'].for_type(
                C,
                lambda obj, p, cycle: p.text(str(obj) if not cycle else '...')
                )

        import sys as _sys
        import inspect as _inspect

        _clsmembers = _inspect.getmembers(_sys.modules[__name__],
                                          _inspect.isclass)

        for _clsmember in _clsmembers:
            _set_printer(_clsmember[1])
except:
    pass
