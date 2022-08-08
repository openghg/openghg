import os as _os
import sys as _sys

from . import (
    analyse,
    cloud,
    dataobjects,
    objectstore,
    retrieve,
    service,
    standardise,
    store,
    types,
    util,
)
from ._version import get_versions  # type: ignore

__all__ = [
    "analyse",
    "cloud",
    "dataobjects",
    "objectstore",
    "retrieve",
    "service",
    "standardise",
    "store",
    "util",
    "types",
]

if _sys.version_info.major < 3:
    raise ImportError("openghg requires Python >= 3.8")

if _sys.version_info.minor < 8:
    raise ImportError("openghg requires Python >= 3.8")

# Let's do some quick checks for required environment variables
_cloud = _os.environ.get("OPENGHG_CLOUD", False)
_hub = _os.environ.get("OPENGHG", False)
_openghg_path = _os.environ.get("OPENGHG_PATH", False)

if not (_cloud or _hub):
    if not _openghg_path:
        raise ValueError(
            "No environment variable OPENGHG_PATH found, please set to use the local object store"
        )

del _cloud, _hub, _openghg_path

v = get_versions()

__version__ = v.get("version")
__branch__ = v.get("branch")
__repository__ = v.get("repository")
__revisionid__ = v.get("full-revisionid")

del v, get_versions
