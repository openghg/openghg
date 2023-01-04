import logging
import os as _os
import sys as _sys
from pathlib import Path as _Path

from . import (
    analyse,
    cloud,
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
    "cloud",
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

if _sys.version_info < (3, 8):
    raise ImportError("openghg requires Python >= 3.8")

v = get_versions()

__version__ = v.get("version")
__branch__ = v.get("branch")
__repository__ = v.get("repository")
__revisionid__ = v.get("full-revisionid")

del v, get_versions


from .util import create_config, get_user_config_path  # type: ignore

cloud_env = _os.environ.get("OPENGHG_CLOUD", False)
hub_env = _os.environ.get("OPENGHG_HUB", False)

# Start module level logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logging.captureWarnings(capture=True)

if cloud_env or hub_env:
    logfile_path = "/tmp/openghg.log"
else:
    logfile_path = str(_Path.home().joinpath("openghg.log"))
    conf_path = get_user_config_path()
    if not conf_path.exists():
        print("\nNo configuration file found, please see installation instructions.")

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
consoleHandler.setLevel(logging.INFO)
logger.addHandler(consoleHandler)

del logfile_path, hub_env, cloud_env, create_config, get_user_config_path
