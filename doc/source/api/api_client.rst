=================
client API Detail
=================

These functions are used to call either remote serverless functions in the OpenGHG Cloud or local processes if OpenGHG is being used as a local
install. Calls are routed depending on the setting of the ``OPENGHG_CLOUD`` environment variable. If found it will attempt to call the remote
functions, if it is absent the call will the routed to a local function.

.. module:: openghg.client

Standardisation
===============

These functions handle the processing, standardisation and storage of data in the object store. Currently functions for
observations, footprint and flux / emissions data are available.

.. autofunction:: standardise_surface

.. autofunction:: standardise_bc

.. autofunction:: standardise_flux

.. autofunction:: standardise_footprint


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
