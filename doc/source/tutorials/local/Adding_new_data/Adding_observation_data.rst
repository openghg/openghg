.. _adding-obs-data:
Adding observation data
=======================

This tutorial demonstrates how OpenGHG can be used to process new
measurement data, search the data present and to retrieve this for
analysis and visualisation.

.. _what-is-object-store:

What is Object Store?
-------------------------------

Each object and piece of data in the object store is stored at a specific key, which can be thought of as the address of the data. The data is stored in a bucket which in the cloud is a section of the OpenGHG object store. Locally a bucket is just a normal directory in the user’s filesystem specified by the path given in the configuration file at ~/.config/openghg/openghg.conf.


.. _using-the-tutorial-object-store:

Using the tutorial object store
-------------------------------

An object store is a folder with a fixed structure within which openghg 
can read and write data. To avoid adding the example data we use in this 
tutorial to your normal object store, we need to tell OpenGHG to use a 
separate sandboxed object store that we'll call the tutorial store. To do 
this we use the ``use_tutorial_store`` function from ``openghg.tutorial``. 
This sets the ``OPENGHG_TUT_STORE`` environment variable for this session and 
won't affect your use of OpenGHG outside of this tutorial.

.. code:: ipython3

    from openghg.tutorial import use_tutorial_store

    use_tutorial_store()

1. Adding and standardising data
--------------------------------

.. note::
    Outside of this tutorial, if you have write access to multiple object stores you
    will need to pass the name of the object store you wish to write to to
    the ``store`` argument of the standardise functions.

Source formats
~~~~~~~~~~~~~~

OpenGHG can process and store several source formats in the object store,
including data from the AGAGE, DECC, NOAA, LondonGHG, BEAC2ON networks.
The process of adding data to the object store is called *standardisation*.

To standardise a new data file, you must specify the *source format* and
other details about the data.
For the full list of accepted observation inputs and source formats, call
the function ``summary_source_formats``:

.. ipython:: python

    from openghg.standardise import summary_source_formats

    summary = summary_source_formats()

    ## UNCOMMENT THIS CODE TO SHOW ALL ENTRIES
    # import pandas as pd; pd.set_option('display.max_rows', None)

    summary

There may be multiple source formats for a given site.
For instance, the Tacolneston site in the UK (site code “TAC”) has four entries:

.. ipython:: python

    summary[summary["Site code"] == "TAC"]

DECC network
~~~~~~~~~~~~

We will start by adding data to the object store from Tacolneston, which is a *surface site*
in the DECC network. (Data at surface sites is measured in-situ.)

First we retrieve the raw data.

.. code:: ipython3

    from openghg.tutorial import retrieve_example_data

    data_url = "https://github.com/openghg/example_data/raw/main/timeseries/tac_example.tar.gz"

    tac_data = retrieve_example_data(url=data_url)


Now we add this data to the object store using ``standardise_surface``, passing the
following arguments:

* ``filepaths``: list of paths to ``.dat`` files
* ``site``:  ``"TAC"``, the site code for Tacolneston
* ``source_format``: ``"CRDS"``, the type of data we want to process
* ``network``: ``"DECC"``

.. ipython::

    In [1]: from openghg.standardise import standardise_surface

    @verbatim
    In [2]: decc_results = standardise_surface(filepaths=tac_data, source_format="CRDS", site="TAC", network="DECC")

    @verbatim
    In [3]: decc_results
    Out[3]: {'processed': {'tac.picarro.hourly.54m.dat': {'ch4': {'uuid': 'e2339fdf-c0d5-46b8-b5b9-3d682610e9fe', 'new': True}, 'co2': {'uuid': '1b4603e6-cac2-458c-b47e-e441864b29eb', 'new': True}},
    'tac.picarro.hourly.100m.dat': {'ch4': {'uuid': '2e5935cc-07e3-4c0f-bd7c-8c6e4e2b13b7', 'new': True}, 'co2': {'uuid': '64c020b8-35dd-483f-b38c-99de83ea412d', 'new': True}},
    'tac.picarro.hourly.185m.dat': {'ch4': {'uuid': '13172db7-7859-4f38-90cf-219c1fbe3b99', 'new': True}, 'co2': {'uuid': 'c79a3473-9f50-47d8-83d8-66a62fd085f7', 'new': True}}}}


This extracts the data and metadata from the files,
standardises them, and adds them to our object store.

The returned ``decc_results`` dictionary shows how the data
has been stored: each file has been split into several entries, each with a unique ID (UUID).
Each entry is known as a *Datasource* (see :ref:`note-on-datasources`).

The ``decc_results`` output includes details of the processed data and tells
us that the data has been stored correctly. This will also tell us if
any errors have been encountered when trying to access and standardise
this data.

Multiple stores
~~~~~~~~~~~~~~~

If you have write access to more than one object store you'll need to pass in the name of that store
to the ``store`` argument.
So instead of the standardise_surface call above, we'll tell it to write to our default ``user`` object store. This is our default local object store
created when we run ``openghg --quickstart``.

.. code:: ipython3

    from openghg.standardise import standardise_surface

    decc_results = standardise_surface(filepaths=tac_data, source_format="CRDS", site="TAC", network="DECC", store="user")

The ``store`` argument can be passed to any of the ``standardise`` functions in OpenGHG and is required if you have write access
to more than one store.

AGAGE data
~~~~~~~~~~

OpenGHG can also process data from the `AGAGE network <https://agage.mit.edu/>`_.

The functions that process the AGAGE data expect data to have an
accompanying *precisions file*. For each data file we create a tuple with
the data filename and the precisions filename.

First we retrieve example data from the  Cape Grim station in Australia (site code "CGO"").

.. code:: ipython3

    cgo_url = "https://github.com/openghg/example_data/raw/main/timeseries/capegrim_example.tar.gz"

    capegrim_data = retrieve_example_data(url=cgo_url)

``capegrim_data`` is a list of two file paths, one for the data file and one for the precisions file:

.. code::

    [PosixPath('/Users/bm13805/openghg_store/tutorial_store/extracted_files/capegrim.18.C'),
    PosixPath('/Users/bm13805/openghg_store/tutorial_store/extracted_files/capegrim.18.precisions.C')]

We put the data file and precisions file into a tuple:

.. code:: ipython3

    capegrim_tuple = (capegrim_data[0], capegrim_data[1])

We can add these files to the object store in the same way as the DECC
data by including the right arguments:

* ``filepaths``: tuple (or list of tuples) with paths to data and precision files
* ``site`` (site code): ``"CGO"``
* ``source_format`` (data type): ``"GCWERKS"``
* ``network``: ``"AGAGE"``
* ``instrument``: ``"medusa"``

.. code:: ipython3

    agage_results = standardise_surface(filepaths=capegrim_tuple, source_format="GCWERKS", site="CGO",
                                  network="AGAGE", instrument="medusa")

When viewing ``agage_results`` there will be a large number of
Datasource UUIDs shown due to the large number of gases in each data
file

.. ipython::
   :verbatim:

   In [15]: agage_results
   Out[15]:
   {'processed': {'capegrim.18.C': {'ch4_70m': {'uuid': '200d8a1b-bc41-4f9f-86c4-448c2427d780',
   'new': True},
   'cfc12_70m': {'uuid': 'e507358e-ade3-4c83-914e-e486628640ce', 'new': True},
   'n2o_70m': {'uuid': 'ad381148-76af-4d8c-aaec-f7cc2a0088b7', 'new': True},
   'cfc11_70m': {'uuid': '2563a11b-2a54-4287-8705-670f34330e33', 'new': True},
   'cfc113_70m': {'uuid': '6a6e28d9-4242-4c6f-a71a-0d56915a485b', 'new': True},
   'chcl3_70m': {'uuid': '36af68d9-f421-4feb-9bfd-c719ec603f05', 'new': True},
   'ch3ccl3_70m': {'uuid': 'f096f4c3-e86f-4d99-8a92-e35dd193cfbc',
   'new': True},
   'ccl4_70m': {'uuid': '396be43c-f29a-408e-9a88-c16ffd79da3b', 'new': True},
   'h2_70m': {'uuid': '62045a91-bac9-4b7d-84b8-696ec8484002', 'new': True},
   'co_70m': {'uuid': 'a1bd7ab9-4ae0-46aa-8570-ec961f929431', 'new': True},
   'ne_70m': {'uuid': '950e94fe-6cf9-48e3-b920-275935761885', 'new': True}}}}


.. _note-on-datasources:

Note on Datasources
^^^^^^^^^^^^^^^^^^^

Datasources are objects that are stored in the `object store <https://docs.openghg.org/api/devapi_objectstore.html>`_ that hold the data and metadata associated with
each measurement we upload to the platform.

For example, if we upload a file that contains readings for three gas
species from a single site at a specific inlet height OpenGHG will
assign this data to three different Datasources, one for each species.
Metadata such as the site, inlet height, species, network etc are stored
alongside the measurements for easy searching.

Datasources can also handle multiple versions of data from a single
site, so if scales or other factors change multiple versions may be
stored for easy future comparison.

3. Searching for data
---------------------

Searching the object store
~~~~~~~~~~~~~~~~~~~~~~~~~~

We can search the object store by property using the
``search_surface(...)`` function. This function retrieves metadata from the
data in the object store.

For example we can find all sites which have measurements for carbon
tetrafluoride (“cf4”) using the ``species`` keyword:

.. code:: ipython3

    from openghg.retrieve import search_surface

    cfc_results = search_surface(species="cfc11")
    cfc_results

We could also look for details of all the data measured at the Tacolneston
(“TAC”) site using the ``site`` keyword:

.. code:: ipython3

    tac_results = search_surface(site="tac")
    tac_results

.. code:: ipython3

    tac_results.results

For this site you can see this contains details of each of the species
as well as the inlet heights these were measured at.

Quickly retrieve data
~~~~~~~~~~~~~~~~~~~~~

Say we want to retrieve all the ``co2`` data from Tacolneston, we can
perform perform a search and expect a |SearchResults|_
object to be returned. If no results are found ``None`` is returned.

.. |SearchResults| replace:: ``SearchResults``
.. _SearchResults: https://docs.openghg.org/api/api_dataobjects.html#openghg.dataobjects.SearchResult

.. code:: ipython3

    results = search_surface(site="tac", species="co2")

.. code:: ipython3

    results.results

We can retrieve either some or all of the data easily using the
``retrieve`` function.

.. code:: ipython3

    inlet_54m_data = results.retrieve(inlet="54m")
    inlet_54m_data

Or we can retrieve all of the data and get a list of ``ObsData``
objects.

.. code:: ipython3

    all_co2_data = results.retrieve_all()

.. code:: ipython3

    all_co2_data

4. Retrieving data
------------------

To retrieve the standardised data from the object store there are
several functions we can use which depend on the type of data we want to
access.

To access the surface data we have added so far we can use the
``get_obs_surface`` function and pass keywords for the site code,
species and inlet height to retrieve our data.

In this case we want to extract the carbon dioxide (“co2”) data from the
Tacolneston data (“TAC”) site measured at the “185m” inlet:

.. code:: ipython3

    from openghg.retrieve import get_obs_surface

    co2_data = get_obs_surface(site="tac", species="co2", inlet="185m")

If we view our returned ``obs_data`` variable this will contain:

-  ``data`` - The standardised data (accessed using
   e.g. ``obs_data.data``). This is returned as an `xarray
   Dataset <https://xarray.pydata.org/en/stable/generated/xarray.Dataset.html>`__.
-  ``metadata`` - The associated metadata (accessed using
   e.g. ``obs_data.metadata``).

.. code:: ipython3

    co2_data

We can now make a simple plot using the ``plot_timeseries`` method of
the ``ObsData`` object.

   **NOTE:** the plot created below may not show up on the online
   documentation version of this notebook.

.. code:: ipython3

    co2_data.plot_timeseries()

You can also pass any of ``title``, ``xlabel``, ``ylabel`` and ``units``
to the ``plot_timeseries`` function to modify the labels.

5. Cleanup
----------

If you're finished with the data in this tutorial you can cleanup the
tutorial object store using the ``clear_tutorial_store`` function.

.. code:: ipython3

    from openghg.tutorial import clear_tutorial_store

.. code:: ipython3

    clear_tutorial_store()
