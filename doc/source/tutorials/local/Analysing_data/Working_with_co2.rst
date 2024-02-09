Working with carbon dioxide
===========================

In this tutorial, we will compare carbon dioxide (:math:`\mathrm{CO_2}`)
emissions with flux data.
For carbon dioxide data, the natural diurnal cycle must be taken into
account when comparing any expected measurements against our
observations.

In order to compare our measurements to modelled data for carbon dioxide,
we need high frequency inputs for both the
footprint and flux files. For NAME footprints these should contain an
additional “H_back” dimension to store hourly footprints overall an
initial time period for each release time. For any natural flux data to
compare against, this should have a frequency of less than 24 hours.


.. note::

   Operations involving :math:`\mathrm{CO}_2` and high resolution data may
   run slowly at the moment.

.. note::
   Plots created within this tutorial may not show up on the
   online documentation.

Using the tutorial object store
-------------------------------

As in the :ref:`previous tutorial <using-the-tutorial-object-store>`,
we will use the tutorial object store to avoid cluttering your personal
object store.

.. code:: ipython3

    from openghg.tutorial import use_tutorial_store

    use_tutorial_store()

1. Loading data sources into the object store
---------------------------------------------

We will use a helper function from the ``openghg.tutorial`` submodule
to add surface, flux, and footprint data to the object store.

You might get a message telling you that a file has already been added
if you've been through one of the other tutorials previously.

.. code:: ipython3

    from openghg.tutorial import populate_flux_data, populate_surface_data, populate_footprint_data

.. code:: ipython3

    populate_surface_data()

    populate_flux_data()

    populate_footprint_data()

2. Creating a model scenario
----------------------------

To link our observations to our ancillary data we can create a
``ModelScenario`` object, as shown in the :ref:`previous tutorial <Comparing observations to emissions>`,
using suitable keywords to grab the data from the object store.

.. code:: ipython3

    from openghg.analyse import ModelScenario

    site = "TAC"
    domain = "EUROPE"
    species = "co2"
    height = "185m"
    source_natural = "natural"
    start_date = "2017-07-01"
    end_date = "2017-07-07"

    scenario = ModelScenario(site=site,
                             inlet=height,
                             domain=domain,
                             species=species,
                             source=source_natural,
                             start_date=start_date,
                             end_date=end_date)

We can plot our observation timeseries using the
``ModelScenario.plot_timeseries()`` method as before:

.. code:: ipython3

    scenario.plot_timeseries()

We can also check trace details of the extracted data by checking the
available metadata. For instance for our footprint data we would expect
this to have an associated species and for this to be labelled as “co2”:

.. code:: ipython3

    footprint_metadata = scenario.footprint.metadata
    footprint_species = footprint_metadata["species"]
    print(f"Our linked footprint has an associated species of '{footprint_species}'")

3. Comparing data sources
-------------------------

Once the correct high frequency emissions and footprints have been
linked for our carbon dioxide data, we can start to plot comparisons
between the sources and our measurement data.

.. code:: ipython3

    scenario.plot_comparison(baseline="percentile")

As in the previous tutorial, multiple fluxes can be linked to your
``ModelScenario`` object if required. This can include additional high
frequency (less than 24 hourly) or low frequency flux data. In this case we have
added monthly “fossil fuel” emissions:

.. code:: ipython3

    source_fossil = "ff-edgar-bp"

    scenario.add_flux(species=species,
                      source=source_fossil,
                      domain=domain)

.. code:: ipython3

    fossil_flux = scenario.fluxes[source_fossil]
    fossil_flux

If we plot the modelled measurement comparison, this will stack the
natural and fossil fuel flux sources and combine with the footprint data
in an appropriate way:

.. code:: ipython3

    # scenario.plot_comparison(baseline="percentile", recalculate=True)

4. Cleanup
----------

If you're finished with the data in this tutorial you can cleanup the
tutorial object store using the ``clear_tutorial_store`` function.

.. code:: ipython3

    from openghg.tutorial import clear_tutorial_store

.. code:: ipython3

    clear_tutorial_store()
