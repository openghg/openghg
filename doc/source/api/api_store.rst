================
store API detail
================

Storage
=======

These classes are used to store each type of data in the object store. Each has a static `load` function that loads a version
of itself from the object store. The `read_file` function is then used to read data files, call standardisation functions based on
the format of the data file, collect metadata and then store the data and metadata in the object store.

openghg.store.BoundaryConditions
^^^^^^^^^^^^^^^^^^^^^^^

The ``BoundaryConditions`` class is used to standardise and store boundary conditions data.

.. autoclass:: openghg.store.BoundaryConditions
    :members:
    :private-members:

.. toctree::
   :maxdepth: 1

openghg.store.Emissions
^^^^^^^^^^^^^^^^^^^^^^^

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

openghg.store.ObsColumn
========================

The ``ObsColumn`` class is used to process column / satellite observation data.

.. autoclass:: openghg.store.ObsColumn
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


Metadata Handling
=================

The ``data_handler_lookup`` function is used in the same way as the search functions. It takes any number of
keyword arguments for searching of metadata and a ``data_type`` argument. It returns a :ref:`DataHandler<DataHandler>` object.

.. autofunction:: openghg.store.data_handler_lookup
