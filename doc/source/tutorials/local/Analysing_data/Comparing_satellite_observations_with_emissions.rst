Comparing Satellite Observations to Emissions
=============================================

In this tutorial, we will see how to combine **satellite observation data**
instead of surface data, along with ancillary data (footprints, emissions, boundary conditions),
into a ``ModelScenario``. This allows us to compute modelled outputs and compare them with satellite-based atmospheric measurements.

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
    populate_bc_southamerica
    )

    populate_column_data()
    populate_satellite_footprint()
    populate_flux_data_satellite()
    populate_bc_southamerica()


2. Creating a model scenario
----------------------------

We can now create a ``ModelScenario`` linking satellite observations with ancillary inputs.

.. code:: ipython3

    from openghg.analyse import ModelScenario

    sc = ModelScenario(satellite="gosat",
                   species="ch4",
                   platform="satellite",
                   max_level=3,
                   domain="southamerica",
                   obs_region="brazil")

Using these keywords, this will search the object store and attempt to collect and attach observation(satellite), footprint(satellite), flux and boundary conditions data. This collected data will be attached to your created ModelScenario. For the observations this will be stored as the ModelScenario.obs attribute. This will be an ObsColumnData object which contains metadata and data for your observations.

.. code:: ipython3

    sc.obs

To access the undelying xarray Dataset containing the observation data use

..code:: ipython3

    ds = scenario.obs.data

The ``ModelScenario.footprint`` attribute contains the linked
FootprintData (again, use ``.data`` to extract xarray Dataset):

.. code:: ipython3

    scenario.footprint

And the ``ModelScenario.fluxes`` attribute can be used to access the
FluxData. Note that for ``ModelScenario.fluxes`` this can contain
multiple flux sources and so this is stored as a dictionary linked to
the source name:

.. code:: ipython3

    scenario.fluxes

Finally, this will also search and attempt to add boundary conditions.
The ``ModelScenario.bc`` attribute can be used to access the
BoundaryConditionsData if present.

.. code:: ipython3

    scenario.bc

.. code:: ipython3

    scenario.bc.data.attrs

An interactive plot for the linked observation data can be plotted using
the ``ModelScenario.plot_timeseries()`` method:

.. code:: ipython3

    scenario.plot_timeseries()

You can also set up your own searches and add this data directly.
One benefit of this interface is to reduce searching the database if the
same data needs to be used for multiple different scenarios.

.. code:: ipython3

    from openghg.retrieve import get_obs_column, get_footprint, get_flux

    satellite = "gosat"
    domain = "southamerica"
    obs_region = "brazil"

    obs_column_data = get_obs_column(
        species="ch4",
        max_level=3,
        satellite=satellite,
        start_date="2016-01-01 14:59:12.500000+00:00",
        end_date="2016-01-01 18:10:16.500000+00:00",
        obs_region="brazil",
    )

    fp_column_data = get_footprint(
            satellite=satellite,
            domain=domain,
            obs_region=obs_region,
            start_date="2016-01-01 14:59:12.500000+00:00",
            end_date="2016-01-01 19:10:16.500000+00:00",
            model="name",
        )

    flux_data = get_flux(species="ch4", source="all", domain="southamerica")

.. code:: ipython3

    scenario_direct = ModelScenario(obs=obs_results, footprint=footprint_results, flux=flux_results, bc=bc_results)

.. note::

   You can create your own input objects directly and add these in the
   same way. This allows you to bypass the object store for experimental
   examples. At the moment these inputs need to be ``ObsData``, ``ObsColumnData``, ``FootprintData``, ``FluxData`` or ``BoundaryConditionsData`` objects,
   which can be created using classes from ``openghg.dataobjects``.
   Simpler inputs will be made available.


3. Comparing data sources
-------------------------

Once your ``ModelScenario`` has been created you can then start to use
the linked data to compare outputs. For example we may want to calculate
modelled observations at our site based on our linked footprint and
emissions data:

.. code:: ipython3

    modelled_observations = scenario.calc_modelled_obs()

This could then be plotted directly using the xarray plotting methods:

.. code:: ipython3

    modelled_observations.plot()  # Can plot using xarray plotting methods

The modelled baseline, based on the linked boundary conditions, can also
be calculated in a similar way:

.. code:: ipython3

    modelled_baseline = scenario.calc_modelled_baseline()
    modelled_baseline.plot()  # Can plot using xarray plotting methods

To compare these modelled observations to the observations
themselves, the ``ModelScenario.plot_comparison()`` method can be used.
This will stack the modelled observations and the modelled baseline by
default to allow comparison:

.. code:: ipython3

    scenario.plot_comparison()

The ``ModelScenario.footprints_data_merge()`` method can also be used to
created a combined output, with all aligned data stored directly within
an ``xarray.Dataset``:

.. code:: ipython3

    combined_dataset = scenario.footprints_data_merge()
    combined_dataset

When the same calculation is being performed for multiple methods, the
last calculation is cached to allow the outputs to be produced more
efficiently. This can be disabled for large datasets by using
``cache=False``.

For a ``ModelScenario`` object, different analyses can be performed on
this linked data. For example if a daily average for the modelled
observations was required, we could calculate this by setting our
``resample_to`` input to ``"1D"`` (matching available pandas time
aliases):

.. code:: ipython3

    modelled_observations_daily = scenario.calc_modelled_obs(resample_to="1D")
    modelled_observations_daily.plot()

Explicit resampling of the data can be also be skipped by using a ``resample_to`` input
of ``None``. This will align the footprints to the observations by forward filling the
footprint values. Note: using ``platform="flask"`` will turn on this option as well.

.. code:: ipython3

    modelled_observations_align = scenario.calc_modelled_obs(resample_to=None)
    modelled_observations_align.plot()

To allow comparisons with multiple flux sources, more than one flux
source can be linked to your ``ModelScenario``. This can be either be
done upon creation or can be added using the ``add_flux()`` method. When
calculating modelled observations, these flux sources will be aligned in
time and stacked to create a total output:

.. code:: ipython3

    scenario.add_flux(species=species, domain=domain, source="energyprod")

.. code:: ipython3

    scenario.plot_comparison()

Output for individual sources can also be created by specifying the
``sources`` as an input:

.. code:: ipython3

    # Included recalculate option to ensure this is updated from cached data.
    modelled_obs_energyprod = scenario.calc_modelled_obs(sources="energyprod", recalculate=True)
    modelled_obs_energyprod.plot()

*Plotting functions to be added for 2D / 3D data*

4. Cleanup
----------

If you're finished with the data in this tutorial you can cleanup the
tutorial object store using the ``clear_tutorial_store`` function.

.. code:: ipython3

    from openghg.tutorial import clear_tutorial_store

.. code:: ipython3

    clear_tutorial_store()
