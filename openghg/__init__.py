from . import (
    analyse,
    dataobjects,
    client,
    localclient,
    objectstore,
    process,
    retrieve,
    service,
    util,
)
import sys as _sys
from ._version import get_versions  # type: ignore

__all__ = [
    "client",
    "dataobjects",
    "localclient",
    "objectstore",
    "process",
    "retrieve",
    "service",
    "store",
    "util",
]

if _sys.version_info.major < 3:
    raise ImportError("openghg requires Python 3.7 minimum")

if _sys.version_info.minor < 7:
    raise ImportError("openghg requires Python 3.7 minimum")

v = get_versions()

__version__ = v.get("version")
__branch__ = v.get("branch")
__repository__ = v.get("repository")
__revisionid__ = v.get("full-revisionid")

del v, get_versions
