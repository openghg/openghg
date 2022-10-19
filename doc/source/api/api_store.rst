================
store API detail
================

openghg.store.Emissions
=======================

The ``Emissions`` class is used to process emissions / flux data files.

.. autoclass:: openghg.store.Emissions
    :members:
    :private-members:

.. toctree::
   :maxdepth: 1

openghg.store.EulerianModel
===========================

The ``EulerianModel`` class is used to process Eulerian model data.

.. autoclass:: openghg.store.EulerianModel
    :members:
    :private-members:

.. toctree::
   :maxdepth: 1

openghg.store.Footprints
===========================

The ``Footprints`` class is used to store and retrieve meteorological data from the ECMWF data store.
Some data may be cached locally for quicker access.

.. autoclass:: openghg.store.Footprints
    :members:
    :private-members:

.. toctree::
   :maxdepth: 1


openghg.store.ObsSurface
========================

The ``ObsSurface`` class is used to process surface observation data.

.. autoclass:: openghg.store.ObsSurface
    :members:
    :private-members:

.. toctree::
   :maxdepth: 1


Recombination functions
=======================

These handle the recombination of data retrieved from the object store.

.. autofunction:: openghg.store.recombine_datasets

.. autofunction:: openghg.store.recombine_multisite



Segmentation functions
======================

These handle the segmentation of data ready for storage in the object store.

.. autofunction:: openghg.store.assign_data
