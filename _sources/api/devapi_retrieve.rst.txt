========
Retrieve
========

These handle the retrieval of data from the object store.

.. module:: openghg.retrieve

Search functions
================

We have a number of search functions, most customised to the data type, which we hope will make it easier
for users to find the data they require from the object store.

Surface observations
^^^^^^^^^^^^^^^^^^^^

To search for surface observations we recommend the use of ``search_surface``.

.. autofunction:: openghg.retrieve.search_surface

Flux data
^^^^^^^^^

.. autofunction:: openghg.retrieve.search_flux

Boundary conditions data
^^^^^^^^^^^^^^^^^^^^^^^^

.. autofunction:: openghg.retrieve.search_bc

Eulerian data
^^^^^^^^^^^^^

.. autofunction:: openghg.retrieve.search_eulerian

Column / satellite data
^^^^^^^^^^^^^^^^^^^^^^^

.. autofunction:: openghg.retrieve.search_column

Footprints
^^^^^^^^^^
.. autofunction:: openghg.retrieve.search_footprints

General
^^^^^^^

For a more general search you can use the ``search`` function directly. This function accepts any number of keyword arguments.

.. autofunction:: openghg.retrieve.search

Retrieving from other data sources
==================================

ICOS
^^^^

OpenGHG can retrieve data from the `ICOS Carbon Portal <https://www.icos-cp.eu/observations/carbon-portal>`_.

.. autofunction:: openghg.retrieve.icos.retrieve_atmospheric

.. autofunction:: openghg.retrieve.icos.retrieve

CEDA
^^^^

Pulling from the CEDA archive is also possible. After finding the URL to the dataset you require you can retrieve it using

.. autofunction:: openghg.retrieve.ceda.retrieve_surface


Specific retrieval functions
============================

.. autofunction:: openghg.retrieve.get_obs_surface

.. autofunction:: openghg.retrieve.get_flux

.. autofunction:: openghg.retrieve.get_footprint

.. autofunction:: openghg.retrieve.get_bc
