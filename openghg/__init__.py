"""
.. currentmodule:: openghg

"""
import sys
from . import client, jobs, localclient, modules, objectstore, processing, service, util
from ._version import get_versions

__all__ = ["client", "jobs", "localclient", "modules", "objectstore", "processing", "service", "util"]

if sys.version_info.major < 3:
    raise ImportError("openghg requires Python 3.6 minimum")

if sys.version_info.minor < 6:
    raise ImportError("openghg requires Python 3.6 minimum")

v = get_versions()

__version__ = v['version']
__branch__ = v['branch']
__repository__ = v['repository']
__revisionid__ = v['full-revisionid']

del v, get_versions
