import logging
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
_hub = _os.environ.get("OPENGHG_HUB", False)
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

import os as _os
from pathlib import Path as _Path

cloud_env = _os.environ.get("OPENGHG_CLOUD", False)
hub_env = _os.environ.get("OPENGHG_HUB", False)

if cloud_env or hub_env:
    logfile_path = "/tmp/openghg.log"
else:
    logfile_path = str(_Path.home().joinpath("openghg.log"))

# Start module level logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Create file handler for log file - set to DEBUG (maximum detail)
fileHandler = logging.FileHandler(logfile_path)  # May want to update this to user area
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

del logfile_path, hub_env, cloud_env
