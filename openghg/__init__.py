import logging
import sys as _sys
from pathlib import Path as _Path

from rich.logging import RichHandler as _RichHandler
from . import (
    analyse,
    dataobjects,
    objectstore,
    datapack,
    retrieve,
    plotting,
    standardise,
    store,
    types,
    tutorial,
    util,
)

__all__ = [
    "analyse",
    "dataobjects",
    "objectstore",
    "datapack",
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

# Use importlib.metadata for version information at runtime
try:
    from importlib.metadata import version as _version, PackageNotFoundError as _PackageNotFoundError

    __version__ = _version("openghg")
except _PackageNotFoundError:
    # Fallback version if package metadata is not available
    __version__ = "unknown"

# These attributes are no longer available with the new versioning approach
# Set to None for backward compatibility
__branch__ = None
__repository__ = None
__revisionid__ = None

# Start module level logging
logger = logging.getLogger("openghg")
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
