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

.. _boundary-contributions:

Boundary contributions
----------------------

``baseline_sensitivities`` multiplies the particle weight in each boundary
curtain cell by its selected boundary concentration, including the existing
species-lifetime loss where applicable. It returns ``bc_n`` and ``bc_s`` on
``(time, lon, height)`` and ``bc_e`` and ``bc_w`` on ``(time, lat, height)``;
dimension order follows the inputs. These arrays retain native footprint
release times and spatial cells for subsequent summation or basis functions.

The default ``bc_time_method="release_time"`` selects concentration at each
release time. With ``bc_time_method="mean_age"``, each cell instead selects at
release time minus its own ``mean_age_particles_*`` value, preserving
fractional ages. Active ages need units convertible to time, such as ``hours``.
Missing, nonfinite or negative ages at positive-weight cells are rejected;
zero ages are valid. Zero-weight cells contribute zero without needing valid
ages or concentration support. The lag changes concentration timing and
retains the existing lifetime loss once.

The mean-age lookup holds a concentration throughout its interval,
including its start and excluding its end. Interval support is normally read
automatically from CF time bounds or, in ``ModelScenario``, the linked boundary
object's ``metadata["time_period"]``. The helper also accepts
``bc.attrs["time_period"]`` as a fallback. Without bounds, timestamps are
treated as interval starts; set ``bc_time_period`` (for example ``"3h"`` or
``"1D"``) to supply or correct a duration if the metadata is missing or
incorrect. CF bounds take precedence over a declared period. Cadence alone
does not establish whether values are interval means or instantaneous
samples, or how their timestamps label intervals; callers must check the
source convention before choosing this lookup. No interpolation, age rounding
or extrapolation is performed.
Unsupported targets, including internal gaps and insufficient earlier
history, raise an error at positive-weight cells. ``allow_climatology``
controls calendar mapping for reference-year boundary fields.

Both ``ModelScenario.calc_modelled_baseline`` and
``ModelScenario.footprints_data_merge`` accept these options. In mean-age mode
the helper computes cellwise products at native release times before the
scenario aligns or averages the completed contributions to observations.
Retrieve enough boundary history for every lagged target and attach it using
``bc=`` or ``add_bc(bc=...)``; the scenario's observation start date alone does
not establish sufficient history. See the :doc:`comparison tutorial
</tutorials/local/Analysing_data/Comparing_with_emissions>` for an example.
Use ``recalculate=True`` when changing the timing method, period, linked data,
output units or requested sensitivities on an already calculated scenario.
Result attributes record ``bc_time_method`` and, for mean-age mode,
``bc_time_policy`` and either ``bc_time_bounds`` or the effective
``bc_time_period``.

This approximation uses each cell's mean age. It does not reconstruct
particle exit-time distributions or change particle normalization, interior
endpoint contributions or downstream boundary-coefficient periods.

.. autofunction:: openghg.analyse.baseline_sensitivities

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
