"""
Acquire: (C) Christopher Woods 2018

This module implements everything necessary to build an
Acquire Accounting Service
"""

from ._account import *
from ._accounts import *
from ._balance import *
from ._errors import *
from ._transaction import *
from ._transactionrecord import *
from ._accounting_service import *
from ._creditnote import *
from ._debitnote import *
from ._pairednote import *
from ._lineitem import *
from ._receipt import *
from ._decimal import *
from ._transactioninfo import *
from ._ledger import *
from ._refund import *

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
