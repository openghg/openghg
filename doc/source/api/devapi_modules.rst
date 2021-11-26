=======================
Development API modules 
=======================

Processing is done by the following classes, each has a customised function to process
the data format from that network / group.

CRANFIELD
=========

.. autoclass:: openghg.modules.CRANFIELD
    :members:
    :private-members:

CRDS
====

.. autoclass:: openghg.modules.CRDS
    :members:
    :private-members:

EUROCOM
=======

.. autoclass:: openghg.modules.EUROCOM
    :members:
    :private-members:

GCWERKS
=======

.. autoclass:: openghg.modules.GCWERKS
    :members:
    :private-members:
    
NOAA
====

.. autoclass:: openghg.modules.NOAA
    :members:
    :private-members:

THAMESBARRIER
=============

.. autoclass:: openghg.modules.THAMESBARRIER
    :members:
    :private-members:


BaseStore
==========

The base module handles the base functionality required of all the processing or data storage classes.
These include the saving, loading and retrieval of the object from its JSON storage format in the object store.

.. autoclass:: openghg.modules.BaseStore
    :members:
    :private-members:


Datasource
==========

Datasources are the basic data storage objects of OpenGHG. They handle the storage of the data as binary NetCDF data in the
object store. They also control the versioning of data and its chunking.

.. autoclass:: openghg.modules.Datasource
    :members:
    :private-members:


Object store
============

Users should never need to use any of the functions in the ``objectstore`` submodule. These handle the storage of
data in the object store, in JSON or binary format.

.. autofunction:: openghg.objectstore.delete_object

.. autofunction:: openghg.objectstore.exists

.. autofunction:: openghg.objectstore.get_bucket

.. autofunction:: openghg.objectstore.get_local_bucket

.. autofunction:: openghg.objectstore.get_object

.. autofunction:: openghg.objectstore.get_object_from_json

.. autofunction:: openghg.objectstore.set_object_from_file

.. autofunction:: openghg.objectstore.set_object_from_json



.. toctree::
   :maxdepth: 1