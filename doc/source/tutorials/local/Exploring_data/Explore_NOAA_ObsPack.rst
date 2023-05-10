Explore NOAA ObsPack
====================

The NOAA ObsPack products are collections of observation data from many
sites which have been collated and standardised. ObsPack data products
are prepared by NOAA in consultation with data providers. Available
ObsPack products can be accessed and downloaded from the `ObsPack
download <https://gml.noaa.gov/ccgg/obspack/data.php>`__ page.

In this tutorial, we will demonstrate how the NOAA ObsPack can be loaded
into the object store, explored and plotted.

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

1. Loading the NOAA ObsPack data
--------------------------------

Download the data
~~~~~~~~~~~~~~~~~

For convenience we have included a copy of the
“obspack_ch4_1_GLOBALVIEWplus_v2.0_2020-04-24” to be retrieved from our
example database. Once this has been downloaded, this can be processd
using the ``add_noaa_obspack()`` function available from
``openghg.store`` subpackage. The file is ~ 128 MB in size so might take
a short time to download depending on your internet connection.

.. code:: ipython3

    from openghg.tutorial import retrieve_example_obspack
    from openghg.store import add_noaa_obspack

    noaa_obspack_directory = retrieve_example_obspack()

Now we have the data retrieved and extracted we can ask OpenGHG to process it and add it to our local object store.

Process and store the data
~~~~~~~~~~~~~~~~~~~~~~~~~~

We pass the directory containing the ObsPack data to ``add_noaa_obspack`` which processes each of the files it finds in the directory.

.. code:: ipython3

    res = add_noaa_obspack(data_directory=noaa_obspack_directory)


2. Search, retrieve and plot
----------------------------

The object store should now contain a large amount of data, we can query the object store and find all the flask CH4 data for
example

.. code:: ipython3

    from openghg.retrieve import search_surface

    flask_ch4 = search_surface(species="ch4", measurement_type="flask", data_source="noaa_obspack")

Then we can look at the results print as a Pandas DataFrame by doing

.. code:: ipython3

    flask_ch4.results

Say we want to have a better look at some of the data from Sary Taukum, Kazakhstan

.. code:: ipython3

    kzd_data = flask_ch4.retrieve(site="kzd")

    kzd_data.plot_timeseries()


3. Using ``get_obs_surface``
----------------------------

Alternatively, we can do an all in one search and retrieve using
``get_obs_surface``. Here we find CH4 data from Estevan Point, British
Columbia, retrieve it and plot it.

.. code:: ipython3

    from openghg.retrieve import get_obs_surface

    data = get_obs_surface(site="HPB", species="ch4")

As there isn't any ranking data set (see tutorial 2) ``get_obs_surface``
doesn't know which inlet to select, we need to tell it.

.. code:: ipython3

    data = get_obs_surface(site="HPB", species="ch4", inlet="93m")

.. code:: ipython3

    data.plot_timeseries()

4. Cleanup
----------

If you're finished with the data in this tutorial you can cleanup the
tutorial object store using the ``clear_tutorial_store`` function.

.. code:: ipython3

    from openghg.tutorial import clear_tutorial_store

    clear_tutorial_store()
