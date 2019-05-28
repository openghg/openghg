"""
Acquire : (C) Christopher Woods 2018

This module contains the interfaces to the object store that provides the
storage of all state for the system. As such, this is the foundation
module that is unlikely to be used by the user, but instead is used
by most of the other modules.
"""

from ._objstore import *
from ._parregistry import *
from ._encoding import *
from ._function import *
from ._mutex import *
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
