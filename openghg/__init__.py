from . import (
    analyse,
    dataobjects,
    client,
    objectstore,
    standardise,
    retrieve,
    service,
    store,
    util,
    types,
)
import sys as _sys
from ._version import get_versions  # type: ignore
import logging

__all__ = [
    "analyse",
    "client",
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
    raise ImportError("openghg requires Python 3.7 minimum")

if _sys.version_info.minor < 7:
    raise ImportError("openghg requires Python 3.7 minimum")

v = get_versions()

__version__ = v.get("version")
__branch__ = v.get("branch")
__repository__ = v.get("repository")
__revisionid__ = v.get("full-revisionid")

del v, get_versions

# Start module level logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Create file handler for log file - set to DEBUG (maximum detail)
fileHandler = logging.FileHandler('openghg.log')  # May want to update this to user area
fileFormatter = logging.Formatter("%(asctime)s:%(levelname)s:%(name)s:%(message)s")
fileHandler.setFormatter(fileFormatter)
fileHandler.setLevel(logging.DEBUG)
logger.addHandler(fileHandler)

# Create console handler - set to WARNING (lower level)
consoleHandler = logging.StreamHandler()
consoleFormatter = logging.Formatter("%(levelname)s:%(name)s:%(message)s")
consoleHandler.setFormatter(consoleFormatter)
consoleHandler.setLevel(logging.WARNING)
logger.addHandler(consoleHandler)
