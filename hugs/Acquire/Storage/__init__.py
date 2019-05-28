"""
Acquire: (C) Christopher Woods 2018

This module provides everything needed to implement a simple interface
to a storage system. A storage system provides space to store files,
based on permissions routed via the access service

"""

from Acquire.Identity import ACLRule, ACLRules

from ._storage_service import *
from ._errors import *
from ._buckethandle import *
from ._userdrives import *
from ._fileinfo import *
from ._driveinfo import *
from ._filemeta import *
from ._drivemeta import *

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
