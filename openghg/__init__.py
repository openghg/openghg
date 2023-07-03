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

util.add_file_handler(logger, logfile_path)
util.add_stream_handler(logger)

del logfile_path, hub_env, cloud_env
