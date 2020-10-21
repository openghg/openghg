=========================
client API Detail
=========================

Process
=======

The ``Process`` class is used to upload locally stored files to cloud for processing.

.. autoclass:: openghg.client.Process
    :members:
    :private-members:

Search
======

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

JobRunner
=========

The ``JobRunner`` class is used to run jobs on HPC clusters.

.. autoclass:: openghg.client.JobRunner
    :members:
    :private-members:

.. toctree::
   :maxdepth: 1