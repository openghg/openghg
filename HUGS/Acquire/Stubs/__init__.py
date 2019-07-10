"""
Acquire : (C) Christopher Woods 2019

This module provides the stubs around modules that are not installed
or available on this system, e.g. for license incompatibility reasons

(i.e. while Acquire is Apache licensed, some users may want to use it
 in systems where GPL modules are installed)
"""

try:
    import lazy_import
    lazy_import.logging.disable(lazy_import.logging.DEBUG)
except:
    # lazy_import is not available, e.g. because we want the Apache
    # licensed version of this code - import the non-lazy wrapper
    from ._lazy_import import *

requests = lazy_import.lazy_module("requests")
