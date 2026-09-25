Adding CO2 satellite data
=========================

This tutorial shows how to add carbon dioxide (:math:`\mathrm{CO_2}`)
satellite footprints to an OpenGHG object store. It assumes that the satellite
column observations have already been standardised; see :ref:`adding-obs-data`
for the observation workflow.

CO2 satellite footprints can be stored in one of two forms. Choose the form
that matches the LPDM output rather than the observation platform: both forms
can be associated with either a surface site or a satellite.

Time-resolved and integrated footprints
---------------------------------------

A time-resolved footprint contains an ``H_back`` dimension. Each value records
the sensitivity for a particular period back from the release time. In the
PARIS/FLEXPART format, OpenGHG stores the recent, time-resolved component as
``fp_time_resolved`` and the remaining older component as ``fp_residual``.
When modelled observations are calculated, OpenGHG multiplies the recent
component by fluxes at the corresponding times and combines it with the
residual contribution. This retains the CO2 diurnal cycle.

An integrated footprint is the single variable ``fp``. It represents the
sensitivity integrated over the whole particle back trajectory, so it has no
``H_back`` dimension and cannot resolve the timing of recent fluxes. OpenGHG
uses its integrated-footprint calculation for this data. For integrated CO2,
it uses monthly-mean fluxes by default, rather than one instantaneous flux at
the release time; this avoids treating a long integrated sensitivity as though
it represented a single point in the diurnal cycle.

Adding a time-resolved CO2 satellite footprint
-----------------------------------------------

For a time-resolved PARIS or FLEXPART file, use ``time_resolved=True``. This
expects ``srr_time_resolved`` and ``srr_residual`` in addition to ``srr`` in
the input, and stores the time-resolved variables described above.

.. code:: ipython3

    from openghg.standardise import standardise_footprint

    standardise_footprint(
        filepath="oco2_footprint.nc",
        source_format="paris",
        satellite="oco2",
        obs_region="china",
        domain="eastasia",
        model="name",
        inlet="column",
        species="co2",
        time_resolved=True,
        store="user",
    )

``satellite`` and ``obs_region`` identify a satellite footprint. OpenGHG sets
``continuous=False`` for satellite data because retrieval times are generally
irregular. Use ``site`` instead of ``satellite`` and ``obs_region`` for a
site-associated footprint.

Adding an integrated CO2 satellite footprint
---------------------------------------------

To add an integrated footprint, explicitly pass ``time_resolved=False``. This
selects the integrated storage and analysis path even for CO2, and retains only
the integrated ``srr`` variable (stored by OpenGHG as ``fp``).

.. code:: ipython3

    standardise_footprint(
        filepath="oco2_footprint.nc",
        source_format="paris",
        satellite="oco2",
        obs_region="china",
        domain="eastasia",
        model="name",
        inlet="column",
        species="co2",
        time_resolved=False,
        store="user",
    )

If ``time_resolved`` is omitted for CO2, OpenGHG keeps the historical default
and expects a time-resolved footprint. Set it explicitly so the stored schema
and the later modelled-observation calculation match the data you supplied.

Using the footprint in a model scenario
----------------------------------------

Create a satellite scenario in the usual way. ``ModelScenario`` chooses the
calculation from the stored footprint variables: time-resolved data goes through
the time-resolved CO2 calculation, while ``fp``-only data goes through the
integrated calculation.

.. important::

   For comparison to the modelled observations, ``max_level`` must match the fixed value used
   when the column footprint was processed.
   Using a different value for the column observations places the observations and footprint
   in different vertical spaces. For an existing footprint, inspect ``footprint.metadata["max_level"]``
   (or ``footprint.data.attrs["max_level"]``) and use that value when retrieving the column
   observations and creating ``ModelScenario``. The example footprint below uses
   ``max_level=17``.

.. code:: ipython3

    from openghg.analyse import ModelScenario

    scenario = ModelScenario(
        satellite="oco2",
        obs_region="china",
        species="co2",
        domain="eastasia",
        source="natural",
        platform="satellite",
        max_level=17,
        time_resolved=False,
    )

    modelled_observations = scenario.calc_modelled_obs()
