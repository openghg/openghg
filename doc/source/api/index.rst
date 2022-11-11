========
User API
========

This section of the documentation gives an overview of the public facing functions used in the Jupyter notebooks available at the
OpenGHG hub. For developers documentation of the internal workings of the library are available in the developer API section.

Data standardisation
====================

The ``standardisation`` submodule handles the standardisation of different data types to the OpenGHG specification.

.. toctree::
   :maxdepth: 2

   api_standardise

Retrieval
=========

The ``retrieve`` submodule contains the functionality needed to retrieve different data types from the OpenGHG store.

.. toctree::
   :maxdepth: 3

   api_retrieve

Plotting
========

These are some helper functions to quickly create plots using ObsData objects.

Classes within the client module are used to interact with OpenGHG, whether it be a cloud or local instance.

.. toctree::
   :maxdepth: 3

   api_plotting
