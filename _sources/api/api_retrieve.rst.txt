===================
Retrieval functions
===================

These handle the retrieval of data from the object store.

.. module:: openghg.retrieve

Search functions
================

We have a number of search functions, most customised to the data type, which we hope will make it easier
for users to find the data they require from the object store.

To search for surface observations we recommend the use of ``search_surface``.

.. autofunction:: openghg.retrieve.search_surface

For a more general search you can use the ``search`` function directly. This function accepts any number of keyword arguments.

.. autofunction:: openghg.retrieve.search

Specific retrieval functions
============================

.. autofunction:: openghg.retrieve.get_obs_surface

.. autofunction:: openghg.retrieve.get_flux

.. autofunction:: openghg.retrieve.get_footprint

.. autofunction:: openghg.retrieve.get_bc
