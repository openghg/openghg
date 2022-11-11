========
User API
========

This section of the documentation gives an overview of the public facing functions used in the Jupyter notebooks available at the
OpenGHG hub. For developers documentation of the internal workings of the library are available in the developer API section.

Analyse
=======

The `analyse` submodule contains functionality to help you analyse data retrieved from the object store.
The `ModelScenario` class allows users to collate related data sources and calculate
modelled output based on this data.

.. toctree::
   :maxdepth: 2

   api_analyse

Data objects
============

Classes used to return data from the object store, help with manipulation and modification.

.. toctree::
   :maxdepth: 2

   api_dataobjects

Plotting
========

These are some helper functions to quickly create plots using ObsData objects.

Classes within the client module are used to interact with OpenGHG, whether it be a cloud or local instance.

.. toctree::
   :maxdepth: 2

   api_plotting

Retrieve
========

The ``retrieve`` submodule contains the functionality needed to retrieve different data types from the OpenGHG store.

.. toctree::
   :maxdepth: 2

   api_retrieve

Standardie
==========

The ``standardise`` submodule handles the standardisation of different data types to the OpenGHG specification.

.. toctree::
   :maxdepth: 2

   api_standardise

Transform
=========

Functions to help transforming of data, regridding, transform of EDGAR data etc.

.. toctree::
   :maxdepth: 2

   api_transform

Tutorial
========

Functions used in our OpenGHG tutorial notebooks and documentation.

.. toctree::
   :maxdepth: 2

   api_tutorial
