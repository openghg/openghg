
import os
import sys
from Acquire.Stubs import lazy_import as _lazy_import

Client = _lazy_import.lazy_module("HUGS.Client")
Service = _lazy_import.lazy_module("HUGS.Service")

__version__ = "0.0.1"

__all__ = ["Client", "Service"]


# TODO - this will be removed in the future, currently using a testing branch of Acquire
if os.path.isdir("../acquire"):
    sys.path.insert(0, "../acquire")
    import Acquire
else:
    expected_path = os.path.abspath("../../acquire")
    raise ImportError("Please clone Acquire into the directory " + expected_path)
