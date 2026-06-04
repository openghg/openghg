import sys as _sys

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
from openghg.util._logging import configure_logger
from openghg.util._user import get_dot_openghg_path

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
from importlib.metadata import version as _version, PackageNotFoundError

try:
    __version__ = _version("openghg")
except PackageNotFoundError:
    # Fallback version if package metadata is not available
    __version__ = "unknown"

# These attributes are no longer available with the new versioning approach
# Set to None for backward compatibility
__branch__ = None
__repository__ = None
__revisionid__ = None

# Configure the logger
# the log files live in subdir "logs" of the path where
# the OpenGHG config is stored.
default_log_path = get_dot_openghg_path() / "logs"
logger = configure_logger(default_log_path)
