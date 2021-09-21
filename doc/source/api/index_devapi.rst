=============
Developer API
=============

The functions and methods documented in this section are the internal workings of the OpenGHG library. They are subject to change
without warning due to the early stages of development of the project.

.. warning:: Normal users should not use any of the functions shown here directly as they may be removed or their functionality may change.

modules
=======

Base class
^^^^^^^^^^

This provides the functionality required by all data storage and processing classes, namely the saving, retrieval and loading
of data from the object store.

:class:`~openghg.modules.BaseModule`
    Base class which the other core processing modules inherit

Data processing
^^^^^^^^^^^^^^^

These classes are used for the processing of data by the ``ObsSurface`` processing class. 

:class:`~openghg.modules.CRANFIELD`
    For processing data from Cranfield
:class:`~openghg.modules.CRDS`
    For processing data from CRDS (cavity ring-down spectroscopy) data from the DECC network.
:class:`~openghg.modules.EUROCOM`
    For processing data from the EUROCOM network
:class:`~openghg.modules.GCWERKS`
    For processing data in the form expected by the GCWERKS package
:class:`~openghg.modules.ICOS`
    For processing data from the ICOS network
:class:`~openghg.modules.NOAA`
    For processing data from the NOAA network
:class:`~openghg.modules.THAMESBARRIER`
    For processing data from the Thames Barrier measurement sites

Datasource
^^^^^^^^^^

The Datasource is the smallest data provider within the OpenGHG topology. A Datasource represents a data provider such as an instrument
measuring a specific gas at a specific height at a specific site. For an instrument measuring three gas species at an inlet height of 100m
at a site we would have three Datasources.

:class:`~openghg.modules.Datasource`
    Handles the storage of data, metadata and version information for measurements


objectstore
============

These functions handle the storage of data in the object store, in JSON or binary format. Each object and piece of data in the
object store is stored at a specific ``key``, which can be thought of as the address of the data. The data is stored
in a ``bucket`` which in the cloud is a section of the OpenGHG object store. Locally a ``bucket`` is just a normal
directory in the user's filesystem specific by the ``OPENGHG_PATH`` environment variable.

:func:`~openghg.objectstore.delete_object`
    Delete an object in the store
:func:`~openghg.objectstore.exists`
    Check if an object exists at that key
:func:`~openghg.objectstore.get_bucket`
    Get path to bucket
:func:`~openghg.objectstore.get_local_bucket`
    Get path to local bucket
:func:`~openghg.objectstore.get_object`
    Get object at given key
:func:`~openghg.objectstore.get_object_from_json`
    Get object from JSON
:func:`~openghg.objectstore.set_object_from_file`
    Set data at a key from a given filepath
:func:`~openghg.objectstore.set_object_from_json`
    Set data at a key from JSON


.. toctree::
   :maxdepth: 1