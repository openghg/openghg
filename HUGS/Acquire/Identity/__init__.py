"""
Acquire : (C) Christopher Woods 2018

This module provides the classes and functions necessary to implement
an Acquire Identity Service. The Identity Service is responsible for
identifying and authenticating users.
"""

from ._aclrule import *
from ._aclrules import *
from ._identity_service import *
from ._loginsession import *
from ._authorisation import *
from ._useraccount import *
from ._usercredentials import *
from ._errors import *

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
