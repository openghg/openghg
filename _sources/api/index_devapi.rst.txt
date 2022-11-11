=============
Developer API
=============

The functions and methods documented in this section are the internal workings of the OpenGHG library. They are subject to change, we'll add
deprecation warnings to functions if we're going to phase them out in the next few releases.

.. warning:: Normal users should not use any of the functions shown here directly as they may be removed or their functionality may change.

Standardisation
===============

Surface measurements
^^^^^^^^^^^^^^^^^^^^

These functions take surface measurement data and standardise it for storage in the object store. They ensure the correct metadata and attributes
are recorded with the data, and that the data is `CF compliant <https://cfconventions.org/>`__. They are called by the ``ObsSurface`` class.

.. toctree::
   :maxdepth: 2

   devapi_standardise

Metadata
^^^^^^^^

These handle the assignment and checking of metadata.

.. toctre::
    :maxdepth: 2

    devapi_metadata

Storage
=======

These functions and classes handle the lower level storage and retrieval of data from the object store.

.. toctree::
   :maxdepth: 2

   devapi_store

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

    api_util

Custom Data Types
=================

Some customised errors and types for type hinting and keeping ``mypy`` happy.

.. toctree::
    :maxdepth: 2

    api_types
