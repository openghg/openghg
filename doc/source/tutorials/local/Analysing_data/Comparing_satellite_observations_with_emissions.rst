Comparing Satellite Observations to Emissions
=============================================

In this tutorial, we will see how to combine **satellite observation data**
instead of surface data, along with ancillary data (footprints, emissions, boundary conditions),
into a ``ModelScenario``. This allows us to compute modelled outputs and compare them
with satellite-based atmospheric measurements.

This tutorial builds on the tutorials :ref:`Adding observation data`
and :ref:`Adding ancillary spatial data`.

.. note::
   Plots created within this tutorial may not show up in static or online documentation outputs.

Using the tutorial object store
-------------------------------

As in the :ref:`previous tutorials <using-the-tutorial-object-store>`, we will use the
tutorial object store to avoid cluttering your personal object store.

.. code:: ipython3

    from openghg.tutorial import use_tutorial_store

    use_tutorial_store()

Omit this step if you're analysing data in your local object store.

1. Loading data sources into the object store
---------------------------------------------

We begin by adding **satellite observation**, **footprint**, **flux**, and (optionally)
**boundary conditions** data to the object store.

We'll use helper functions from ``openghg.tutorial`` to populate example data:

.. code:: ipython3

    from openghg.tutorial import (
        populate_column_data,
        populate_satellite_footprint,
        populate_flux_data_satellite,
,
    )

    populate_column_data()
    populate_satellite_footprint()
    populate_flux_data_satellite()

2. Creating a model scenario
----------------------------

We can now create a ``ModelScenario`` linking satellite observations with ancillary inputs.

.. code:: ipython3

    from openghg.analyse import ModelScenario

    species = "ch4"
    domain = "southamerica"
    satellite="GOSAT"
    obs_region="brazil"
    height = "column"
    source = "all"


    scenario = ModelScenario(satellite=satellite,
                            platform="satellite",
                            max_level=3,
                            obs_region=obs_region,
                            inlet=height,
                            model="name",
                            domain=domain,
                            species=species,
                            source=source,
                            )

Check attached data:

.. code:: ipython3

    scenario.obs
    scenario.footprint
    scenario.fluxes

To view the underlying observation data:

.. code:: ipython3

    ds = scenario.obs.data

Plot time series:

.. code:: ipython3

    scenario.plot_timeseries()

3. Comparing data sources
-------------------------

Calculate modelled observations using emissions and footprints:

.. code:: ipython3

    modelled_observations = scenario.calc_modelled_obs()
    modelled_observations.plot()

Calculate the modelled baseline using boundary conditions:

.. code:: ipython3

    modelled_baseline = scenario.calc_modelled_baseline()
    modelled_baseline.plot()

Compare modelled data to observations:

.. code:: ipython3

    scenario.plot_comparison()

Merge and align data into one Dataset:

.. code:: ipython3

    combined_dataset = scenario.footprints_data_merge()
    combined_dataset

Resample to daily resolution:

.. code:: ipython3

    modelled_obs_daily = scenario.calc_modelled_obs(resample_to="1D")
    modelled_obs_daily.plot()

Disable resampling (forward-fill footprints):

.. code:: ipython3

    modelled_obs_aligned = scenario.calc_modelled_obs(resample_to=None)
    modelled_obs_aligned.plot()


.. note::

   Satellite data (e.g. XCH4) is column-integrated and may have different spatiotemporal sensitivity than surface data.

5. Cleanup
----------

If you're done with the tutorial data, clean up the object store:

.. code:: ipython3

    from openghg.tutorial import clear_tutorial_store
    clear_tutorial_store()
