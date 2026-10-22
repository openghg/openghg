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

Experimental boundary reference arithmetic
-----------------------------------------

The private ``openghg.analyse._modelled_baseline.baseline_reference_sensitivities``
function supports the continuous-surface CO2/O2 preparation pilot in OpenGHG
Inversions. Its interface may change before a public analysis API is released.
It leaves the existing ``ModelScenario`` boundary calculation unchanged.

Supply a fixed reference concentration and its units, together with an explicit
acceptable absolute error in total particle mass. All four curtain weights are
validated and normalized together per observation. The returned native curtain
operands are ``bc_anomaly_<direction>`` and ``bc_mass_<direction>``, plus scalar
``bc_reference``. Their absolute contribution is ``anomaly + reference * mass``.
Anomalies and mass remain separate so later preparation can apply the same spatial
and temporal maps before choosing an inference reference.

Boundary timestamps denote interval starts; the boundary dataset must declare
``time_period`` (for example, ``1 month``). Forward filling outside those intervals
is rejected. Shared spatial coordinates must match exactly. The helper supports
stable CO2/O2 only and does not apply reactive loss. Float64 arithmetic cannot
recover digits already lost in supplied concentrations or weights. Borrowed
arrays and metadata remain unchanged; Dask-backed value validation runs when
the operands are computed.
