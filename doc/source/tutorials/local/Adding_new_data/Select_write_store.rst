Select a store to write to
==========================

This tutorial assumes you have write priviliges to multiple object stores and for the purposes of the
tutorial we'll create a new config file with multiple stores to detail this process.

0. Backing up your config file
------------------------------

To avoid deletion of your current config file we'll first create a backup

.. code:: bash

    $ cp ~/.config/openghg/openghg.conf $ cp ~/.config/openghg/openghg.conf.bak


1. Create a new config
-----------------------

We'll now run ``openghg --quickstart`` and create a new configuration file with two object stores,
both in our home directory.

.. code-block:: bash

    $ openghg --quickstart

    OpenGHG configuration
    ---------------------

    INFO:openghg.util:We'll first create your user object store.

    Enter path for your local object store (default /home/gareth/openghg_store):
    Would you like to add another object store? (y/n): y
    Enter the name of the store: shared
    Enter the object store path: /home/gareth/openghg_shared

    You will now be asked for read/write permissions for the store.
    For read only enter r, for read and write enter rw.

    Enter object store permissions: rw
    Would you like to add another object store? (y/n): n
    INFO:openghg.util:Configuration written to /home/gareth/.config/openghg/openghg.conf

Now that we have two object stores we have write permissions to we can try standardising some surface
measurements from the DECC network.

2. Standardising data
---------------------

We'll retrieve some surface data from the Tacolneston site from our example data repository.

.. code:: ipython3

    from openghg.tutorial import retrieve_example_data

    data_url = "https://github.com/openghg/example_data/raw/main/timeseries/tac_example.tar.gz"

    tac_data = retrieve_example_data(url=data_url)

First we'll try using the :func:`openghg.standardise.standardise_surface` function to standardise this data.

.. code:: ipython3

    from openghg.standardise import standardise_surface

    tac_result = standardise_surface(filepaths=tac_data, source_format="CRDS", site="TAC", network="DECC")

    ...
    ObjectStoreError: More than one writable store, stores we can write to are: user, shared.

The whole error has been removed above but the result is the same, OpenGHG doesn't know which store to write to.
We want to write this data to the shared store so everyone in our group can access it.
Let's call ``standardise_surface`` again but this time pass in the ``store`` argument so OpenGHG knows where to write
the data to.

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
