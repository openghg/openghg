=======
Analyse
=======

The ModelScenario class allows users to collate related data sources and calculate
modelled output based on this data. The types of data currently included are:
- Timeseries observation data (:ref:`ObsData<ObsData>`)
- Fixed domain sensitivity maps known as footprints (:ref:`FootprintData<FootprintData>`)
- Fixed domain flux maps (:ref:`FluxData<FluxData>`) - multiple maps can be included and referenced by source name
- Fixed domain vertical curtains at the boundaries referred to as boundary conditions (:ref:`BoundaryConditionsData<BoundaryConditionsData>`)

.. autoclass:: openghg.analyse.ModelScenario
    :members:

Footprint times flux
--------------------

``fp_x_flux_time_resolved_numba`` computes source-resolved model sensitivity without
summing the spatial grid. It returns a lazy ``(source, lat, lon, time)`` array,
so basis functions can be applied later. When observation times are supplied,
selection happens after the full lagged calculation is constructed. Install
the optional kernel dependency with ``pip install 'openghg[fp-x-flux]'``.
Single-source flux and regularly spaced coarse flux are supported; footprint
release times must currently be a regular hourly grid.

.. autofunction:: openghg.analyse.fp_x_flux_time_resolved_numba

.. autofunction:: openghg.analyse.write_fp_x_flux_zarr

Numba compiles the kernel on first use in each process. For benchmarks or a
distributed cluster, the optional warm-up can be run on every worker with
``client.run(warm_numba_fp_x_flux)``. It is not required for correctness.

.. autofunction:: openghg.analyse.warm_numba_fp_x_flux

Private stable boundary operands
-------------------------------

The development-only helper
``openghg.analyse._boundary_operands.baseline_reference_sensitivities``
prepares CO2/O2 boundary curtains for downstream centered calculations. It is
not exported as a public API. It jointly normalizes the four native particle
curtains and returns per-cell anomaly, normalized mass, and an explicit scalar
reference, so downstream models can retain arbitrary spatial bases and choose
their own time periods. Absolute contributions reconstruct as
``bc_anomaly_d + bc_reference * bc_mass_d`` for each direction ``d``.

The caller supplies the arithmetic reference, its units, and an absolute
``mass_tolerance`` in ``[0, 1)``. Boundary timestamps declare interval starts
with a positive ``time_period``; forward filling is restricted to those
intervals. Spatial indexes must match exactly. Concentrations and references
use explicit convertible mole-fraction units; particle weights have unscaled
dimensionless units. Computation uses float64 and preserves Dask laziness and
borrowed inputs. Array-value checks run when lazy outputs are materialized.
Precision already lost in input data cannot be recovered.

This helper supports stable CO2/O2 only. It introduces neither climatological
coverage nor short-lived-species loss handling. It is separate from the monthly
residual-flux correction delivered in PR #1790.
