"""
Acquire: (C) Christopher Woods 2018

This module provides everything needed to implement an Acquire Accounting
Service

"""

from ._access_service import *
from ._errors import *
from ._checksum import *
from ._request import *
from ._runrequest import *
from ._jobsheet import *

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
