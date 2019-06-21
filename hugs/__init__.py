
from Acquire.Stubs import lazy_import as _lazy_import

Client = _lazy_import.lazy_module("hugs.Client")
Service = _lazy_import.lazy_module("hugs.Service")

__version__ = "0.0.1"

__all__ = ["Client", "Service"]

