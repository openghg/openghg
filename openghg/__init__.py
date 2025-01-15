import logging
import sys as _sys
from pathlib import Path as _Path

from rich.logging import RichHandler as _RichHandler
from . import (
    analyse,
    dataobjects,
    objectstore,
    retrieve,
    plotting,
    standardise,
    store,
    types,
    tutorial,
    util,
)
from ._version import get_versions  # type: ignore

__all__ = [
    "analyse",
    "dataobjects",
    "objectstore",
    "retrieve",
    "plotting",
    "standardise",
    "store",
    "types",
    "tutorial",
    "util",
]

if _sys.version_info < (3, 10):
    raise ImportError("openghg requires Python >= 3.10")

v = get_versions()

__version__ = v.get("version")
__branch__ = v.get("branch")
__repository__ = v.get("repository")
__revisionid__ = v.get("full-revisionid")

del v, get_versions

# Start module level logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logging.captureWarnings(capture=True)

logfile_path = str(_Path.home().joinpath("openghg.log"))

# Create file handler for log file - set to DEBUG (maximum detail)
fileHandler = logging.FileHandler(logfile_path)  # May want to update this to user area
fileFormatter = logging.Formatter(
    "%(asctime)s:%(levelname)s:%(name)s:%(message)s", datefmt="%Y-%m-%dT%H:%M:%S%z"
)
fileHandler.setFormatter(fileFormatter)
fileHandler.setLevel(logging.DEBUG)
logger.addHandler(fileHandler)

# Create console handler - set to WARNING (lower level)
consoleHandler = _RichHandler()
consoleFormatter = logging.Formatter("%(levelname)s:%(name)s:%(message)s", datefmt="%Y-%m-%dT%H:%M:%S%z")
consoleHandler.setFormatter(consoleFormatter)
consoleHandler.setLevel(logging.INFO)
logger.addHandler(consoleHandler)

del logfile_path
