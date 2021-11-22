=================
client API Detail
=================

JobRunner
=========

The ``JobRunner`` class is used to run jobs on HPC clusters.

.. autoclass:: openghg.client.JobRunner
    :members:
    :private-members:

.. toctree::
   :maxdepth: 1

Process
=======

The ``Process`` class is used upload data for processing by OpenGHG.

.. autoclass:: openghg.client.Process
    :members:
    :private-members:

Search (Client)
===============

The ``Search`` class is used to search for data in the object store.

.. autoclass:: openghg.client.Search
    :members:
    :private-members:

RankSources
===========

The ``RankSources`` class is used to rank sources of data. Setting a high rank for a Datasource across a specific daterange means
that data from that Datasource will be preferred when a user searches for data.

.. autoclass:: openghg.client.RankSources
    :members:
    :private-members:

Retrieve
===========

The ``Retrieve`` class is used to retrieve data from the object store.

.. autoclass:: openghg.client.Retrieve
    :members:
    :private-members:


