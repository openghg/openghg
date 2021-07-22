from . import client, jobs, localclient, modules, objectstore, processing, service, util
from ._version import get_versions  # type: ignore

__all__ = ["client", "jobs", "localclient", "modules", "objectstore", "processing", "service", "util"]

v = get_versions()

__version__ = v.get("version")
__branch__ = v.get("branch")
__repository__ = v.get("repository")
__revisionid__ = v.get("full-revisionid")

del v, get_versions
