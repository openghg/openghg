Searching and plotting
======================

In this short tutorial we'll show how to retrieve some data and create a
simple plot using one of our plotting functions.

Using the tutorial object store
-------------------------------

As in the :ref:`previous tutorial <using-the-tutorial-object-store>`,
we will use the tutorial object store to avoid cluttering your personal
object store.

.. code:: ipython3

    from openghg.tutorial import use_tutorial_store

    use_tutorial_store()

Now we'll add some data to the tutorial store.

.. code:: ipython3

    from openghg.tutorial import populate_surface_data
    populate_surface_data()

1. Searching
-------------

Let's search for all the methane data from Tacolneston.
To do this we need to know the site code ("TAC").

If we didn't know the site code, we could find it using
the ``summary_site_codes()`` function:

.. ipython:: python

    from openghg.standardise import summary_site_codes

    ## UNCOMMENT THIS CODE TO SHOW ALL ENTRIES
    # import pandas as pd; pd.set_option('display.max_rows', None)

    summary = summary_site_codes()
    summary

The output of this function is a `pandas
DataFrame <https://pandas.pydata.org/pandas-docs/stable/user_guide/dsintro.html#dataframe>`__,
so we can filter to find sites containing the name “Tacolneston”:

.. ipython:: python

    site_long_name = summary["Long name"]
    find_tacolneston = site_long_name.str.contains("Tacolneston")

    summary[find_tacolneston]

This shows us that the site code for Tacolneston is "TAC", and also that
there are two entries for Tacolneston, since it is included under
multiple networks.

To see all available data associated with Tacolneston we
can search for this using the site code of “TAC”.

.. code:: ipython3

    from openghg.retrieve import search

    tac_data_search = search(site="tac")

For our search we can take a look at the ``results`` property (which is
a pandas DataFrame).

.. code:: ipython3

    tac_data_search.results

To just look for the surface observations we can use the
``search_surface`` function specifically. We can also pass multiple keys
to extract, for example, just the methane data:

.. code:: ipython3

    from openghg.retrieve import search_surface

    tac_surface_search = search_surface(site="TAC", species="ch4")
    tac_surface_search.results

There are also equivalent search functions for other data types
including ``search_footprints``, ``search_emissions`` and ``search_bc``.

2. Plotting
-----------------

If we want to take a look at the data from the 185m inlet we can first
retrieve the data from the object store and then create a quick
timeseries plot. See the |SearchResults|_ object documentation for more information.

.. |SearchResults| replace:: ``SearchResults``
.. _SearchResults: https://docs.openghg.org/api/api_dataobjects.html#openghg.dataobjects.SearchResult

.. code:: ipython3

    data_185m = tac_surface_search.retrieve(inlet="185m")

.. note::
   The plots created below may not show up on the online
   documentation version of this notebook.

We can visualise this data using the in-built plotting commands from the
``plotting`` sub-module. We can also modify the inputs to improve how
this is displayed:

.. code:: ipython3

    from openghg.plotting import plot_timeseries

    plot_timeseries(data_185m, title="Methane at Tacolneston", xlabel="Time", ylabel="Concentration", units="ppm")

.. raw:: html

   <iframe src="images/tac_surface_185m.html" width="100%" height="400"></iframe>

Plotting multiple timeseries
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If there are multiple results for a given search, we can also retrieve
all the data and receive a ``list`` of |ObsData|_ objects.

.. |ObsData| replace:: ``ObsData``
.. _ObsData: https://docs.openghg.org/api/api_dataobjects.html#openghg.dataobjects.ObsData

.. code:: ipython3

    all_ch4_tac = tac_surface_search.retrieve()

Then we can use the ``plot_timeseries`` function from the ``plotting``
submodule to compare measurements from different inlets. This creates a
`Plotly <https://plotly.com/python/>`__ plot that should be interactive
and and responsive, even with relatively large amounts of data.

.. code:: ipython3

    plot_timeseries(data=all_ch4_tac, units="ppb")

.. raw:: html

   <iframe src="images/tac_surface_all.html" width="100%" height="400"></iframe>

3. Comparing different sites
-----------------------------

We can easily compare data for the same species from different sites by
doing a quick search to see what's available

.. code:: ipython3

    ch4_data = search_surface(species="ch4")

    ch4_data.results

Then we refine our search to only retrieve the sites (and inlets) that
we want to compare and make a plot

.. code:: ipython3

    bsd_data = ch4_data.retrieve(site="BSD")
    tac_data = ch4_data.retrieve(site="TAC", inlet="54m")

.. code:: ipython3

    plot_timeseries(data=[bsd_data, tac_data], title="Comparing CH4 measurements at Tacolneston and Bilsdale")

.. raw:: html

   <iframe src="images/bsd_tac_ch4.html" width="100%" height="400"></iframe>


