Adding observation data
=======================

This tutorial demonstrates how OpenGHG can be used to process new
measurement data, search the data present and to retrieve this for
analysis and visualisation.

0. Using the tutorial object store
----------------------------------

To avoid adding the example data we use in this tutorial to your normal
object store, we need to tell OpenGHG to use a separate sandboxed object
store that we'll call the tutorial store. To do this we use the
``use_tutorial_store`` function from ``openghg.tutorial``. This sets the
``OPENGHG_TUT_STORE`` environment variable for this session and won't
affect your use of OpenGHG outside of this tutorial.

.. code:: ipython3

    from openghg.tutorial import use_tutorial_store

    use_tutorial_store()

1. Adding and standardising data
--------------------------------

Data types
~~~~~~~~~~

Within OpenGHG there are several data types which can be processed and
stored within the object store. This includes data from the AGAGE, DECC,
NOAA, LondonGHG, BEAC2ON networks.

When uploading a new data file, the data type must be specified
alongside some additional details so OpenGHG can recognise the format
and the correct standardisation can occur. The details needed will vary
by the type of data being uploaded but will often include the
measurement reference (e.g. a site code) and the name of any network.

For the full list of accepted observation inputs and data types, there
is a summary function which can be called:

.. code:: ipython3

    from openghg.standardise import summary_source_formats

    summary = summary_source_formats()

    ## UNCOMMENT THIS CODE TO SHOW ALL ENTRIES
    # import pandas as pd; pd.set_option('display.max_rows', None)

    summary

Note: there may be multiple data types applicable for a give site. This
is can be dependent on various factors including the instrument type
used to measure the data e.g. for Tacolneston (“TAC”):

.. code:: ipython3

    summary[summary["Site code"] == "TAC"]

DECC network
~~~~~~~~~~~~

We will start by adding data to the object store from a surface site
within the DECC network. Here we have accessed a subset of data from the
Tacolneston site (site code “TAC”) in the UK.

.. code:: ipython3

    from openghg.tutorial import retrieve_example_data

    data_url = "https://github.com/openghg/example_data/raw/main/timeseries/tac_example.tar.gz"

    tac_data = retrieve_example_data(url=data_url)

As this data is measured in-situ, this is classed as a surface site and
we need to use the ``ObsSurface`` class to interpret this data. We can
pass our list of files to the ``read_file`` method associated within the
``ObsSurface`` class, also providing details on: - site code - ``"TAC"``
for Tacolneston - type of data we want to process, known as the data
type - ``"CRDS"`` - network - ``"DECC"``

This is shown below:

.. code:: ipython3

    from openghg.standardise import standardise_surface

    decc_results = standardise_surface(filepaths=tac_data, source_format="CRDS", site="TAC", network="DECC")

.. code:: ipython3

    print(decc_results)

Here this extracts the data (and metadata) from the supplied files,
standardises them and adds these to our created object store.

The returned ``decc_results`` will give us a dictionary of how the data
has been stored. The data itself may have been split into different
entries, each one stored with a unique ID (UUID). Each entry is known as
a *Datasource* (see below for a note on Datasources). The
``decc_results`` output includes details of the processed data and tells
us that the data has been stored correctly. This will also tell us if
any errors have been encountered when trying to access and standardise
this data.

AGAGE data
~~~~~~~~~~

Another data type which can be added is data from the AGAGE network. The
functions that process the AGAGE data expect data to have an
accompanying precisions file. For each data file we create a tuple with
the data filename and the precisions filename. *Note: A simpler method
of uploading these file types is planned.*

We can now retrieve the example data for Capegrim as we did above

.. code:: ipython3

    cgo_url = "https://github.com/openghg/example_data/raw/main/timeseries/capegrim_example.tar.gz"

.. code:: ipython3

    capegrim_data = retrieve_example_data(url=cgo_url)

.. code:: ipython3

    capegrim_data

We must create a ``tuple`` associated with each data file to link this
to a precision file:

.. code:: python

   list_of_tuples = [(data1_filepath, precision1_filepath), (data2_filepath, precision2_filepath), ...]

.. code:: ipython3

    capegrim_data.sort()
    capegrim_tuple = (capegrim_data[0], capegrim_data[1])

The data being uploaded here is from the Cape Grim station in Australia,
site code “CGO”.

We can add these files to the object store in the same way as the DECC
data by including the right keywords: - site code - ``"CGO"`` for Cape
Grim - data type - ``"GCWERKS"`` - network - ``"AGAGE"``

.. code:: ipython3

    agage_results = standardise_surface(filepaths=capegrim_tuple, source_format="GCWERKS", site="CGO",
                                  network="AGAGE", instrument="medusa")

When viewing ``agage_results`` there will be a large number of
Datasource UUIDs shown due to the large number of gases in each data
file

.. code:: ipython3

    agage_results

A note on Datasources
^^^^^^^^^^^^^^^^^^^^^

Datasources are objects that are stored in the object store (++add link
to object store notes++) that hold the data and metadata associated with
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
``search_surface(...)`` function.

For example we can find all sites which have measurements for carbon
tetrafluoride (“cf4”) using the ``species`` keyword:

.. code:: ipython3

    from openghg.retrieve import search_surface

    cfc_results = search_surface(species="cfc11")
    cfc_results

We could also look for details of all the data measured at the Billsdale
(“BSD”) site using the ``site`` keyword:

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
perform perform a search and expect a
```SearchResults`` <https://docs.openghg.org/api/api_dataobjects.html#openghg.dataobjects.SearchResult>`__
object to be returned. If no results are found ``None`` is returned.

.. code:: ipython3

    results = search_surface(site="tac", species="co2")

.. code:: ipython3

    results.results

We can retrive either some or all of the data easily using the
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
   e.g. ``obs_data.data``). This is returned as an `xarray
   Dataset <https://xarray.pydata.org/en/stable/generated/xarray.Dataset.html>`__.
-  ``metadata`` - The associated metadata (accessed using
   e.g. ``obs_data.metadata``).

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
