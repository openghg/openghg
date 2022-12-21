Searching and plotting
======================

In this short tutorial we'll show how to retrieve some data and create a
simple plot using one of our plotting functions.

As in the `previous tutorial <Adding_observation_data.ipynb>`__, we will
start by setting up our temporary object store for our data. If you've
already create your own local object store you can skip the next few
steps and move onto the **Searching** section.

.. code:: ipython3

    from openghg.tutorial import populate_surface_data

.. code:: ipython3

    populate_surface_data()

Searching
---------

Let's search for all the methane data from Tacolneston

.. code:: ipython3

    from openghg.retrieve import search_surface

    ch4_results = search_surface(site="tac", species="ch4")
    ch4_results

Let's take a look at the results property which is a pandas DataFrame
object.

.. code:: ipython3

    ch4_results.results

If we want to take a look at the data from the 185m inlet we can first
retrieve the data from the object store and then create a quick
timeseries plot. See the
```SearchResults`` <https://docs.openghg.org/api/api_dataobjects.html#openghg.dataobjects.SearchResults>`__
object documentation for more information.

.. code:: ipython3

    data_185m = ch4_results.retrieve(inlet="185m")

   **NOTE:** the plots created below may not show up on the online
   documentation version of this notebook.

.. code:: ipython3

    data_185m.plot_timeseries()

You can make some simple changes to the plot using arguments

.. code:: ipython3

    data_185m.plot_timeseries(title="Methane at Tacolneston", xlabel="Time", ylabel="Conc.", units="ppm")

Plot all the data
-----------------

We can also retrieve all the data, get a ``list`` of
```ObsData`` <https://docs.openghg.org/api/api_dataobjects.html#openghg.dataobjects.ObsData>`__
objects.

.. code:: ipython3

    all_ch4_tac = ch4_results.retrieve_all()

Then we can use the ``plot_timeseries`` function from the ``plotting``
submodule to compare measurements from different inlets. This creates a
`Plotly <https://plotly.com/python/>`__ plot that should be interactive
and and responsive, even with relatively large amounts of data.

.. code:: ipython3

    from openghg.plotting import plot_timeseries

    plot_timeseries(data=all_ch4_tac, units="ppb")

Compare different sites
-----------------------

We can easily compare data from different sites by doing a quick search
to see what's available

.. code:: ipython3

    ch4_data = search_surface(species="ch4")

.. code:: ipython3

    ch4_data

Then we refine our search to only retrieve the inlets we want

.. code:: ipython3

    ch4_data.results

.. code:: ipython3

    lower_inlets = search_surface(species="ch4", inlet=["42m", "54m"])

.. code:: ipython3

    lower_inlets

Then we can retrieve all the data and make a plot.

.. code:: ipython3

    lower_inlet_data = lower_inlets.retrieve_all()

.. code:: ipython3

    plot_timeseries(data=lower_inlet_data, title="Comparing CH4 measurements at Tacolneston and Bilsdale")

You can also search for different data types, say we want to find
surface measurement data and emissions data at the same time. We can do
that with the more generic ``search`` function.

We need to first load in some emissions data

.. code:: ipython3

    from openghg.tutorial import populate_flux_data

.. code:: ipython3

    populate_flux_data()

To search across different types we can use the more generic ``search``
function.

.. code:: ipython3

    from openghg.retrieve import search

.. code:: ipython3

    results = search(species="ch4", data_type=["surface", "emissions"])

.. code:: ipython3

    results.results
