============
Data objects
============

DataManager
===========

This ``dataclass`` is used to modify metadata stored in :ref:`Datasource<Datasource>` objects and the metadata store.
``DataManager`` instances are created by the :ref:`data_manager<data_manager>` function.

.. autoclass:: openghg.dataobjects.DataManager
    :members:

SearchResults
=============

This ``dataclass`` is returned by the OpenGHG search functions and allows easy retrieval and querying of metadata retrieved
by the ``search`` function.

.. autoclass:: openghg.dataobjects.SearchResults
    :members:

ObsData
=======

This ``dataclass`` is returned by data retrieval functions such as :ref:`get_obs_surface<get_obs_surface>` and the :ref:`SearchResults<SearchResults>`
retrieve function.

.. autoclass:: openghg.dataobjects.ObsData
    :members:

FluxData
========

This ``dataclass`` is used to return observations data from the :ref:`get_flux<get_flux>` function

.. autoclass:: openghg.dataobjects.FluxData
    :members:

ObsColumnData
=============

This ``dataclass`` is used to return observations data from the :ref:`get_obs_column<get_obs_column>` function

.. autoclass:: openghg.dataobjects.ObsColumnData
    :members:

FootprintData
=============

This ``dataclass`` is used to return observations data from the :ref:`get_footprint<get_footprint>` function

.. autoclass:: openghg.dataobjects.FootprintData
    :members:

BoundaryConditionsData
======================

This ``dataclass`` is used to return observations data from the :ref:`get_bc<get_bc>` function

.. autoclass:: openghg.dataobjects.BoundaryConditionsData
    :members:
