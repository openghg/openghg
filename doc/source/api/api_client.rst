=================
client API Detail
=================

These functions are used to call either remote serverless functions in the OpenGHG Cloud or local processes if OpenGHG is being used as a local
install. Calls are routed depending on the setting of the ``OPENGHG_CLOUD`` environment variable. If found it will attempt to call the remote
functions, if it is absent the call will the routed to a local function.

.. module:: openghg.client

Processing and standardisation
==============================

The ``process_files`` function is used to upload data files for standardisation and storage in the object store

.. autofunction:: openghg.client.process_files

search (client)
===============

The ``search`` function is used to search for and retrieve data from the object store. The :ref:`SearchResults<SearchResults>` object that 
it returns can then be used to retrieve data from the object store.


.. autofunction:: openghg.client.search

get_obs_surface
===============

A helper function for those familiar with the Bristol ACRG repository. This attempts to provide the same interface as the 
equivalnet to the ``get_obs_surface`` as available in that repository. This returns a list of :ref:`ObsData<ObsData>` objects

.. autofunction:: openghg.client.get_obs_surface

rank_sources
============

The ``rank_sources`` is used to rank sources of data. Provided with a site and a species the ``rank_sources`` function will search for the given 
site and species and return a :ref:`RankSources<RankSources>` object

Setting a high rank for a Datasource across a specific daterange means 
that data from that Datasource will be preferred when a user searches for data.

.. autofunction:: openghg.client.rank_sources


JobRunner
=========

The ``JobRunner`` class is used to run jobs on HPC clusters.

.. note::
    This will currently raise a ``NotImplementedError`` if used.

.. autoclass:: openghg.client.JobRunner
    :members:
    :private-members:

.. toctree::
   :maxdepth: 1
