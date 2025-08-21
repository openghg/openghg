Comparing observations to emissions
===================================

In this tutorial, we will see how to combine observation data and
acillary data into a ``ModelScenario``, which can compute modelled
outputs based on ancillary data, and compare these modelled outputs
to observed measurements.

This tutorial builds on the tutorials :ref:`Adding observation data`
and :ref:`Adding ancillary spatial data`.

.. note::
   Plots created within this tutorial may not show up on the
   online documentation version of this notebook.

Using the tutorial object store
-------------------------------

As in the :ref:`previous tutorials <using-the-tutorial-object-store>`,
we will use the tutorial object store to avoid cluttering your personal
object store.

.. jupyter-execute::

    from openghg.tutorial import use_tutorial_store

    use_tutorial_store()

Omit this step if you want to analyse data in your local object store.
(This data needs to be added following the instructions in the
:ref:`previous <Adding observation data>` :ref:`tutorials <Adding ancillary spatial data>`.)


1. Loading data sources into the object store
---------------------------------------------

We begin by adding observation, footprint, flux, and (optionally)
boundary conditions data to the object store.
See :ref:`Adding ancillary spatial data <Adding ancillary spatial data>` for more details
on these inputs.
This data relates to Tacolneston (TAC) site within the DECC
network and the area around Europe (EUROPE domain).

We'll use some helper functions from the ``openghg.tutorial`` submodule
to retrieve raw data in the :ref:`expected format <2. Input format>`:

.. jupyter-execute::

    from openghg.tutorial import populate_surface_data, populate_footprint_inert, populate_flux_ch4, populate_bc_ch4

.. jupyter-execute::
    :hide-output:

    populate_surface_data()

    populate_footprint_inert()

    populate_flux_ch4()

    populate_bc_ch4()

2. Creating a model scenario
----------------------------

With this ancillary data, we can start to make comparisons between model
data, such as bottom-up inventories, and our observations. This analysis
is based around a ``ModelScenario`` object which can be created to link
together observation, footprint, flux / emissions data and boundary conditions
data.

Above we loaded observation data from the Tacolneston site into the
object store. We also added both an associated footprint (sensitivity map)
and an anthropogenic emissions map for a domain defined over Europe.

To access and link this data we can set up our ``ModelScenario``
instance using a similar set of keywords. In this case we have also
limited ourselves to a date range:

.. jupyter-execute::

    from openghg.analyse import ModelScenario

    species="ch4"
    site="tac"
    domain="EUROPE"
    height="100m"
    source_waste = "waste"
    start_date = "2016-07-01"
    end_date = "2016-08-01"

    scenario = ModelScenario(site=site,
                             inlet=height,
                             domain=domain,
                             species=species,
                             source=source_waste,
                             start_date=start_date,
                             end_date=end_date)

Using these keywords, this will search the object store and attempt to
collect and attach observation, footprint, flux and boundary conditions
data. This collected data will be attached to your created
``ModelScenario``. For the observations this will be stored as the
``ModelScenario.obs`` attribute. This will be an ``ObsData`` object
which contains metadata and data for your observations:

.. jupyter-execute::

    scenario.obs

To access the undelying xarray Dataset containing the observation data
use ``ModelScenario.obs.data``:

.. jupyter-execute::

    scenario.obs.data

The ``ModelScenario.footprint`` attribute contains the linked
FootprintData (again, use ``.data`` to extract xarray Dataset):

.. jupyter-execute::

    scenario.footprint

And the ``ModelScenario.fluxes`` attribute can be used to access the
FluxData. Note that for ``ModelScenario.fluxes`` this can contain
multiple flux sources and so this is stored as a dictionary linked to
the source name:

.. jupyter-execute::

    scenario.fluxes

Finally, this will also search and attempt to add boundary conditions.
The ``ModelScenario.bc`` attribute can be used to access the
BoundaryConditionsData if present.

.. jupyter-execute::

    scenario.bc

.. jupyter-execute::

    scenario.bc.data.attrs

An interactive plot for the linked observation data can be plotted using
the ``ModelScenario.plot_timeseries()`` method:

.. jupyter-execute::

    scenario.plot_timeseries()

You can also set up your own searches and add this data directly.
One benefit of this interface is to reduce searching the database if the
same data needs to be used for multiple different scenarios.

.. jupyter-execute::

    from openghg.retrieve import get_obs_surface, get_footprint, get_flux, get_bc

    # Extract obs results from object store
    obs_results = get_obs_surface(site=site,
                                  species=species,
                                  inlet=height,
                                  start_date="2016-07-01",
                                  end_date="2016-08-01")

    # Extract footprint results from object store
    footprint_results = get_footprint(site=site,
                                      domain=domain,
                                      height=height,
                                      start_date="2016-07-01",
                                      end_date="2016-08-01")

    # Extract flux results from object store
    flux_results = get_flux(species=species,
                            domain=domain,
                            source=source_waste,
                            start_date="2016-01-01",
                            end_date="2016-12-31")

    # Extract specific boundary conditions from the object store
    bc_results = get_bc(species=species,
                        domain=domain,
                        bc_input="CAMS",
                        start_date="2016-07-01",
                        end_date="2016-08-01")

.. jupyter-execute::

    scenario_direct = ModelScenario(obs=obs_results, footprint=footprint_results, flux=flux_results, bc=bc_results)

.. note::

   You can create your own input objects directly and add these in the
   same way. This allows you to bypass the object store for experimental
   examples. At the moment these inputs need to be ``ObsData``,
   ``FootprintData``, ``FluxData`` or ``BoundaryConditionsData`` objects,
   which can be created using classes from ``openghg.dataobjects``.
   Simpler inputs will be made available.


3. Comparing data sources
-------------------------

Once your ``ModelScenario`` has been created you can then start to use
the linked data to compare outputs. For example we may want to calculate
modelled observations at our site based on our linked footprint and
emissions data:

.. jupyter-execute::

    modelled_observations = scenario.calc_modelled_obs()

This could then be plotted directly using the xarray plotting methods:

.. jupyter-execute::

    modelled_observations.mf_mod.plot()  # Can plot using xarray plotting methods

The modelled baseline, based on the linked boundary conditions, can also
be calculated in a similar way:

.. jupyter-execute::

    modelled_baseline = scenario.calc_modelled_baseline()
    modelled_baseline.bc_mod.plot()  # Can plot using xarray plotting methods

To compare these modelled observations to the observations
themselves, the ``ModelScenario.plot_comparison()`` method can be used.
This will stack the modelled observations and the modelled baseline by
default to allow comparison:

.. jupyter-execute::

    scenario.plot_comparison()

The ``ModelScenario.footprints_data_merge()`` method can also be used to
created a combined output, with all aligned data stored directly within
an ``xarray.Dataset``:

.. jupyter-execute::

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

.. jupyter-execute::

    modelled_observations_daily = scenario.calc_modelled_obs(resample_to="1D")
    modelled_observations_daily.mf_mod.plot()

Explicit resampling of the data can be also be skipped by using a ``resample_to`` input
of ``None``. This will align the footprints to the observations by forward filling the
footprint values. Note: using ``platform="flask"`` will turn on this option as well.

.. jupyter-execute::

    modelled_observations_align = scenario.calc_modelled_obs(resample_to=None)
    modelled_observations_align.mf_mod.plot()

To allow comparisons with multiple flux sources, more than one flux
source can be linked to your ``ModelScenario``. This can be either be
done upon creation or can be added using the ``add_flux()`` method. When
calculating modelled observations, these flux sources will be aligned in
time and stacked to create a total output:

.. jupyter-execute::

    scenario.add_flux(species=species, domain=domain, source="energyprod")

.. jupyter-execute::

    scenario.plot_comparison()

Output for individual sources can also be created by specifying the
``sources`` as an input:

.. jupyter-execute::

    # Included recalculate option to ensure this is updated from cached data.
    modelled_obs_energyprod = scenario.calc_modelled_obs(sources="energyprod", recalculate=True)
    modelled_obs_energyprod.mf_mod.plot()

*Plotting functions to be added for 2D / 3D data*


4. Sensitivity matrices
-----------------------

To perform an inversion for a scenario, we need `sensitivity matrices` that combine the footprints and flux (or particle locations and boundary conditions).
We can get the "footprint x flux" matrix from ``calc_modelled_obs``:

.. jupyter-execute::

   # use the output_fp_x_flux option, which stores the result in the fp_x_flux data variable
   # we are recalculating to avoid using cached data
   fp_x_flux = scenario.calc_modelled_obs(output_fp_x_flux=True, recalculate=True).fp_x_flux
   fp_x_flux

To get a matrix suitable for typical inversion frameworks, we can flatten the latitude and longitude coordinates, and use the resulting values.

.. jupyter-input::

   h = fp_x_flux.stack(latlon=["lat", "lon"]).values

(Normally you would apply basis functions to reduce the size of the matrix.)


The corresponding calculation for baseline sensitivities from boundary conditions is:

.. jupyter-execute::

   from openghg.analyse._modelled_baseline import baseline_sensitivities

   bc_sensitivity = scenario.calc_modelled_baseline(output_sensitivity=True)
   bc_sensitivity

All of this data (except the baseline sensitivities) can be produced at once using ``footprints_data_merge``:

.. jupyter-execute::

   combined_data = scenario.footprints_data_merge(calc_fp_x_flux=True,
                                                  calc_bc_sensitivity=True,
                                                  recalculate=True)
   data_vars = ["mf", "mf_mod", "bc_mod", "fp_x_flux"] + [f"bc_{d}" for d in "nesw"]
   combined_data[data_vars]

Notice that the units of all these data variables are compatible. We will say more about this in the next section.


5. Working with units
---------------------

You can specify the units you prefer in ``footprints_data_merge`` (look at the attributes of the data variables to see their units):

.. jupyter-execute::

   combined_data = scenario.footprints_data_merge(calc_fp_x_flux=True,
                                                  calc_bc_sensitivity=True,
                                                  recalculate=True,
                                                  output_units="mol/mol")
   data_vars = ["mf", "mf_mod", "bc_mod", "fp_x_flux"] + [f"bc_{d}" for d in "nesw"]
   combined_data[data_vars]

By default, the native units of the obs data are used, but here have used ``"mol/mol"``, which is equivalent to using ``"1"``.
Other options could be floats like ``1e-9``, or ``"1e-9 mol/mol"``, or abbreviations like ``"ppm"``, ``"ppb"``, and ``"ppt"``.
To see the units of the obs data, use ``scenario.units``. If this returns ``None``, then ``mol/mol`` will be used for conversions.

These outputs have aligned units, but they are not `units aware`. To do computations while preserving the units, you can `quantify` the data:

.. jupyter-execute::

   mf1 = combined_data.mf.pint.quantify()
   mf2 = combined_data.mf.pint.quantify().pint.to("ppb")

   # the values are very different
   print(mf1.mean().values, mf2.mean().values)

   # because we have quantified the DataArrays, summing them will automatically align the units
   (mf1 + mf2).pint.to("ppb").mean().values

Note that alignment and reindexing quantified data can be tempermental, so it is safest to align data while it is unquantified, then quantify it to do calculations, then dequantify when you are done.

Also note that the units in ``calc_modelled_obs`` and ``calc_modelled_baseline`` have the same units conversion options as ``footprints_data_merge``.
Further, you can use ``scenario.convert_units(ds)`` to convert the units of a dataset ``ds`` to the units stored in ``scenario.units``.

6. Multi-sector scenarios
-------------------------

Recall that we have added two fluxes to our scenario:

.. jupyter-execute::

   scenario.fluxes

By default, ``calc_modelled_obs`` and ``footprints_data_merge`` sum multiple fluxes into a single total flux.
However, we can choose to do these computations separately:

.. jupyter-execute::

   mod_obs_sectoral = scenario.calc_modelled_obs(output_fp_x_flux=True, split_by_sectors=True, recalculate=True)

   mod_obs_sectoral

Now we have a sensitivity matrix with a ``sector`` dimension:

.. jupyter-execute::

   fp_x_flux_sectoral = mod_obs_sectoral.fp_x_flux_sectoral
   fp_x_flux_sectoral

To get a matrix for use in an inversion, we can stack coordinates:

.. jupyter-input::

   h = fp_x_flux_sectoral.stack(latlonsec=["lat", "lon", "sector"]).values

(Again, you would normally apply basis functions first.)

Cleanup
-------

If you're finished with the data in this tutorial you can cleanup the
tutorial object store using the ``clear_tutorial_store`` function.

.. jupyter-execute::

    from openghg.tutorial import clear_tutorial_store

.. jupyter-execute::

    clear_tutorial_store()
