Searching and plotting
======================

In this short tutorial we’ll show how to retrieve some data and create a
simple plot using one of our plotting functions.

As in the `previous tutorial <Adding_observation_data.ipynb>`__, we will
start by setting up our temporary object store for our data. If you’ve
already create your own local object store you can skip the next few
steps and move onto the **Searching** section.

.. code:: ipython3

    from openghg.tutorial import populate_surface_data

.. code:: ipython3

    populate_surface_data()

Searching
---------

Let’s search for all the methane data from Tacolneston to do this we
need to know the site code. We can see a summary of known site codes
using the ``summary_site_codes()`` function

.. code:: ipython3

    from openghg.standardise import summary_site_codes

    ## UNCOMMENT THIS CODE TO SHOW ALL ENTRIES
    # import pandas as pd; pd.set_option('display.max_rows', None)

    summary = summary_site_codes()
    summary

The output of this function is a `pandas
DataFrame <https://pandas.pydata.org/pandas-docs/stable/user_guide/dsintro.html#dataframe>`__.
If we wanted to filter this to include sites containing the name
“Tacolneston” we could do so as follows:

.. code:: ipython3

    site_long_name = summary["Long name"]
    find_tacolneston = site_long_name.str.contains("Tacolneston")

    summary[find_tacolneston]

As you can see, there will sometimes be multiple entries for a site if
this is included under multiple networks.

If we wanted to see all available data associated with Tacolneston we
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

If we want to take a look at the data from the 185m inlet we can first
retrieve the data from the object store and then create a quick
timeseries plot. See the
```SearchResults`` <https://docs.openghg.org/api/api_dataobjects.html#openghg.dataobjects.SearchResults>`__
object documentation for more information.

.. code:: ipython3

    data_185m = tac_surface_search.retrieve(inlet="185m")

   **NOTE:** the plots created below may not show up on the online
   documentation version of this notebook.

We can visualise this data using the in-built plotting commands from the
``plotting`` sub-module. We can also modify the inputs to improve how
this is displayed:

.. code:: ipython3

    from openghg.plotting import plot_timeseries

    plot_timeseries(data_185m, title="Methane at Tacolneston", xlabel="Time", ylabel="Conc.", units="ppm")

Plot all the data
-----------------

If there are multiple results for a given search, we can also retrieve
all the data and receive a ``list`` of
```ObsData`` <https://docs.openghg.org/api/api_dataobjects.html#openghg.dataobjects.ObsData>`__
objects.

.. code:: ipython3

    all_ch4_tac = tac_surface_search.retrieve()

Then we can use the ``plot_timeseries`` function from the ``plotting``
submodule to compare measurements from different inlets. This creates a
`Plotly <https://plotly.com/python/>`__ plot that should be interactive
and and responsive, even with relatively large amounts of data.

.. code:: ipython3

    plot_timeseries(data=all_ch4_tac, units="ppb")

Compare different sites
-----------------------

We can easily compare data for the same species from different sites by
doing a quick search to see what’s available

.. code:: ipython3

    ch4_data = search_surface(species="ch4")

.. code:: ipython3

    ch4_data

Then we refine our search to only retrieve the sites (and inlets) that
we want:

.. code:: ipython3

    ch4_data.results

We can retrieve the data we want to compare and make a plot

.. code:: ipython3

    bsd_data = ch4_data.retrieve(site="BSD")
    tac_data = ch4_data.retrieve(site="TAC", inlet="54m")

.. code:: ipython3

    plot_timeseries(data=[bsd_data, tac_data], title="Comparing CH4 measurements at Tacolneston and Bilsdale")
