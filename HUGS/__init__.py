
# import os
# import sys

# acquire_path = os.path.join(os.path.dirname(__file__), "../../acquire")
# TODO - this will be removed in the future, currently using a testing branch of Acquire
# if os.path.isdir(acquire_path):
#     sys.path.insert(0, acquire_path)
#     import Acquire
# else:
#     expected_path = os.path.abspath(acquire_path)
#     raise ImportError("Please clone Acquire into the directory " + expected_path)


# from Acquire.Stubs import lazy_import as _lazy_import

# Client = _lazy_import.lazy_module("HUGS.Client")
# Service = _lazy_import.lazy_module("HUGS.Service")

__version__ = "0.0.2"

__all__ = ["Client", "Service", "Modules", "ObjectStore", "Processing", "User", "Util"]

