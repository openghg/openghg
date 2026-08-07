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

``fp_x_flux_keep_space`` computes source-resolved model sensitivity without
summing the spatial grid. It returns a lazy ``(source, lat, lon, time)`` array,
so basis functions can be applied later. When observation times are supplied,
selection happens after the full lagged calculation is constructed. Install
the optional kernel dependency with ``pip install 'openghg[fp-x-flux]'``.
Single-source flux, regularly spaced coarse flux, and irregular footprint
release times are supported.

Flux timestamps are interpreted as the starts of regular averaging intervals.
For a release time and ``H_back`` lag, the target belongs to the half-open
interval ``[flux_time, flux_time + cadence)``. Values are not interpolated,
matched to the nearest timestamp, or extrapolated across missing intervals.
Use ``align_flux_to_time_targets`` when the same explicit alignment is needed
outside the full operator. Non-monotonic release times are sorted internally
for efficient block reads and restored to their exact original positional
order in the result.
The indexed kernel defaults to 32 releases per compute chunk when
``time_chunk`` is omitted; tune this value for the available worker memory and
spatial grid size.

.. autofunction:: openghg.analyse.align_flux_to_time_targets

.. autofunction:: openghg.analyse.fp_x_flux_keep_space

.. autofunction:: openghg.analyse.write_fp_x_flux_keep_space_zarr

Numba compiles the kernel on first use in each process. For benchmarks or a
distributed cluster, the optional warm-up can be run on every worker with
``client.run(warm_numba_fp_x_flux)``. It is not required for correctness.

.. autofunction:: openghg.analyse.warm_numba_fp_x_flux
