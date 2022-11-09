=============
Developer API
=============

The functions and methods documented in this section are the internal workings of the OpenGHG library. They are subject to change
without warning due to the early stages of development of the project.

.. warning:: Normal users should not use any of the functions shown here directly as they may be removed or their functionality may change.

Standardisation
===============

Surface measurements
^^^^^^^^^^^^^^^^^^^^

These functions take surface measurement data and standardise it for storage in the object store. They ensure the correct metadata and attributes
are recorded with the data, and that the data is `CF compliant <https://cfconventions.org/>`__. They are called by the ``ObsSurface`` class.

:func:`~openghg.standardise.surface.parse_aqmesh`
    For processing data from the AQMesh network
:func:`~openghg.standardise.surface.parse_beaco2n`
    For processing data from the BEACO2N network
:func:`~openghg.standardise.surface.parse_btt`
    For processing data from the BT Tower site
:func:`~openghg.standardise.surface.parse_cranfield`
    For processing data from Cranfield
:func:`~openghg.standardise.surface.parse_crds`
    For processing data from CRDS (cavity ring-down spectroscopy) data from the DECC network.
:func:`~openghg.standardise.surface.parse_eurocom`
    For processing data from the EUROCOM network
:func:`~openghg.standardise.surface.parse_gcwerks`
    For processing data in the form expected by the GCWERKS package
:func:`~openghg.standardise.surface.parse_noaa`
    For processing data from the NOAA network
:func:`~openghg.standardise.surface.parse_npl`
    For processing data from NPL
:func:`~openghg.standardise.surface.parse_tmb`
    For processing data from the Thames Barrier site

Metadata handling
^^^^^^^^^^^^^^^^^

These handle the assignment and standardisation of meta`data`.

Attributes
**********

Ensuring the NetCDF created during standardisation has the correct attributes assigned to it.

:func:`~openghg.standardise.meta.assign_attributes`
    Assign attributes to a number of datasets.

:func:`~openghg.standardise.meta.get_attributes`
    Assign attributes to a single dataset, called by the above.

Metadata sync
*************

:func:`~openghg.standardise.meta.sync_surface_metadata`
    Ensure the required metadata is shared between the metadata and attributes.

Storage
=======

These functions and classes handle the lower level storage and retrieval of data from the object store.

.. toctree::
   :maxdepth: 2

   api_store

Dataclasses
===========

These dataclasses are used to return data and metadata from the object store. Some of them also contain helper functions to quickly plot data,
modify metadata and delete data in the object store.

.. toctree::
   :maxdepth: 2

   api_dataobjects

Retrieval functions
===================

These handle the retrieval of data from the object store.

.. toctree::
   :maxdepth: 2

   api_retrieve

Object Store
============

These functions handle the storage of data in the object store, in JSON or binary format. Each object and piece of data in the
object store is stored at a specific ``key``, which can be thought of as the address of the data. The data is stored
in a ``bucket`` which in the cloud is a section of the OpenGHG object store. Locally a ``bucket`` is just a normal
directory in the user's filesystem specified by the path given in the configuration file at ``~/.config/openghg/openghg.conf``.


.. toctree::
   :maxdepth: 2

   api_objectstore

Utility functions
=================

This module contains all the helper functions used throughout OpenGHG.

.. toctree::
    :maxdepth: 2

Custom Data Types
=================

Errors
^^^^^^

Customised errors for OpenGHG.

:class:`~openghg.util.InvalidSiteError`
    Raised if an invalid site is given

:class:`~openghg.util.UnknownDataError`
    Raised if we don't recognize the data passed

:class:`~openghg.util.FunctionError`
    Raised if there has been an error with a serverless function.

:class:`~openghg.util.ObjectStoreError`
    Raised if an error accessing an object at a key in the object store occurs


Types
^^^^^

These are used in conjunction with ``mypy`` to make type hinting easier.

:class:`~openghg.util.pathType`

:class:`~openghg.util.multiPathType`

:class:`~openghg.util.resultsType`
