"""
.. currentmodule:: openghg

"""
from . import client, jobs, localclient, modules, objectstore, processing, service, util
from ._version import get_versions

v = get_versions()

print(v)

__version__ = v['version']
__branch__ = v['branch']
__repository__ = v['repository']
__revisionid__ = v['full-revisionid']

del v, get_versions
