"""
.. currentmodule:: openghg

"""
from . import client, jobs, localclient, modules, objectstore, processing, service, util
from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
