================
store API detail
================

Emissions
=========

The ``Emissions`` class is used to process emissions / flux data files.

.. autoclass:: openghg.modules.Emissions
    :members:
    :private-members:

.. toctree::
   :maxdepth: 1

EulerianModel
=============

The ``EulerianModel`` class is used to process Eulerian model data.

.. autoclass:: openghg.modules.EulerianModel
    :members:
    :private-members:

.. toctree::
   :maxdepth: 1

Footprints
==========

The ``Footprints`` class is used to store and retrieve meteorological data from the ECMWF data store.
Some data may be cached locally for quicker access.

.. autoclass:: openghg.modules.Footprints
    :members:
    :private-members:

.. toctree::
   :maxdepth: 1


METStore
========

The ``METStore`` class is used to find and retrieve meteorological data from the ECMWF data store.
Some data may be cached locally for quicker access.

.. autoclass:: openghg.modules.METStore
    :members:
    :private-members:

.. toctree::
   :maxdepth: 1

ObsSurface
==========

The ``ObsSurface`` class is used to process surface observation data.

.. autoclass:: openghg.modules.ObsSurface
    :members:
    :private-members:

.. toctree::
   :maxdepth: 1


Recombination
=============

These handle the recombination of data retrieved from the object store.

.. autofunction:: openghg.processing.recombine_datasets

.. autofunction:: openghg.processing.recombine_multisite



Segmentation
============

These handle the segmentation of data ready for storage in the object store.

.. autofunction:: openghg.processing.assign_data

