======================
dataobjects API detail
======================

SearchResults
=============

This ``dataclass`` is returned by the ``openghg.client.search`` function and allows easy retrieval and querying of metadata retrieved
by the ``search`` function.

.. autoclass:: openghg.dataobjects.SearchResults
    :members:
    :private-members:

ObsData
=======

This ``dataclass`` is returned by data retrieval functions such as :ref:`get_obs_surface`<get_obs_surface> and the :ref:`SearchResults`<SearchResults> 
retrieve function.

.. autoclass:: openghg.dataobjects.ObsData
    :members:
    :private-members:

FluxData
========

This ``dataclass`` is used to return observations data from the get_flux function

.. autoclass:: openghg.dataobjects.FluxData
    :members:
    :private-members:

FootprintData
=============

This ``dataclass`` is used to return observations data from the get_footprint function

.. autoclass:: openghg.dataobjects.FootprintData
    :members:
    :private-members:


METData
=======

This ``dataclass`` is used to return observations data from the Met data retrieval function

.. autoclass:: openghg.dataobjects.METData
    :members:
    :private-members:


RankSources
===========

This ``dataclass`` is returned by the ``openghg.client.rank_sources`` function and allows easy setting of ranking attributes for data stored in the
object store.

.. autoclass:: openghg.dataobjects.RankSources
    :members:
    :private-members:


