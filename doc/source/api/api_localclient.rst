=========================
localclient API Detail
=========================

get_single_site
===============

Used to search the object store using a site name, species and daterange and returns the data found.

.. autofunction:: openghg.localclient.get_single_site
    

process_files
=============

Used to process files for storage in the local object store.

.. autofunction:: openghg.localclient.process_files
    

RankSources
===========

The ``RankSources`` class is used to rank sources of data. Setting a high rank for a Datasource across a specific daterange means
that data from that Datasource will be preferred when a user searches for data.


.. autoclass:: openghg.localclient.RankSources
    :members:
    :private-members:

Search
======

Search the local object store for data. This will be removed as it has been replaced with the ``get_obs`` function above.


.. autoclass:: openghg.localclient.Search
    :members:
    :private-members:
