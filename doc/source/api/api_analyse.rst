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
so basis functions can be applied later. Install the optional kernel dependency
with ``pip install 'openghg[fp-x-flux]'``. Single-source flux, regularly spaced
coarse flux, and hourly, multi-hour, irregular or non-monotonic footprint release
times are supported. Input data, attributes and chunks are left unchanged,
including when the result is computed.

Declared units are normalized separately for the resolved footprint, residual
footprint and flux. Canonical footprint units are ``m2 s mol-1`` and canonical
flux units are ``mol m-2 s-1``. Compatible scaled units, such as
``m2 s umol-1`` or ``umol m-2 s-1``, are converted lazily before multiplication.
Missing ``units`` attributes assume canonical units for compatibility with older
inputs; incompatible declared units, including ``1``, raise ``ValueError``.
The result contains dimensionless mol/mol values with ``units="1"``.

The default ``cast_float32=True`` converts all three fields to float32 after
unit normalization. With ``cast_float32=False``, they use a common float32 or
float64 dtype that preserves any float64 input and promotes integer inputs.
Unsupported numeric types raise ``ValueError``. Computation precision is separate
from persistence: ``write_fp_x_flux_zarr`` intentionally converts the native
mol/mol result to float32 ppm values.

Flux timestamps must describe the starts of regular averaging intervals with a
positive whole-hour cadence. A singleton flux timestamp represents a one-hour
interval.
For a release time minus a ``H_back`` lag, the target belongs to the half-open
interval ``[flux_time, flux_time + cadence)``. Time values are not interpolated,
matched to the nearest timestamp, or extrapolated across missing intervals.
``H_back`` values are non-negative integer hourly offsets, so the resolved
window is ``max(H_back) + 1`` hours, including for sparse lag coordinates.
Flux whose cadence is strictly greater than this window uses monthly mean flux
for both resolved and residual contributions. Otherwise the resolved term uses
interval membership and the residual term uses monthly means. Each monthly mean
includes all supplied flux values in the release's calendar month, before the
lag-halo data are trimmed. Each release calendar month must contain at least
one supplied native flux value; a supplied record need not cover the complete
month. A month without a native flux label raises ``ValueError``, even if an
interval from the preceding month covers a release. Flux must cover the releases and their full left lag halo in either case.
Use ``align_flux_to_time_targets`` for the same explicit interval alignment
outside the full operator.

Spatial alignment uses nearest-neighbour reindexing on latitude and longitude
with a tolerance of ``1e-5`` in their coordinate units. Cells outside this
tolerance become missing. The default ``fillna_zero=True`` replaces missing
footprint and flux values with zero, including unmatched spatial cells;
``fillna_zero=False`` retains missing values for the calculation.

Non-monotonic release times are sorted internally and restored to their exact
original positional order in the result. Duplicate releases are retained when
``time_selector`` is omitted. A selector is applied after the full lazy graph is
constructed and requires a unique result time coordinate, unique non-missing
selector values, and timestamps present in the result; missing selector values
are discarded. Selected timestamps follow the selector's order.

The indexed kernel defaults to at most 32 releases per compute chunk when
``time_chunk`` is omitted; the regular hourly kernel defaults to one full-time
chunk. For the regular hourly kernel, the wrapper increases compute chunks to
at least the maximum hourly lag and merges an undersized final compute chunk,
then applies the requested output chunks. Latitude, longitude
and source chunk sizes default to their full dimensions.

.. autofunction:: openghg.analyse.align_flux_to_time_targets

.. autofunction:: openghg.analyse.fp_x_flux_time_resolved_numba

The wrapper handles unit normalization, common-dtype conversion, missing values,
spatial alignment, source selection, sorting and chunk preparation. The
computation-only ``fp_x_flux_time_resolved_numba_core`` entry point requires
Dask-backed inputs with monotonic non-decreasing release time, exact spatial
alignment, a ``source`` dimension on flux, and one common float32 or float64
dtype. Declared units must already have canonical dimensions and scale; absent
unit attributes assume canonical units. The core validates these contracts and
does not normalize units, cast, fill missing values, select sources, sort or
rechunk inputs.

Core resolved and residual footprints must share coordinates and time/spatial
chunks; flux spatial chunks must match those of the footprint. ``H_back`` must
occupy one chunk. The core uses the indexed kernel when prepared release time
chunks are too small for the regular halo kernel. This preserves prepared
release time chunks, including small final chunks.

.. autofunction:: openghg.analyse.fp_x_flux_time_resolved_numba_core

.. autofunction:: openghg.analyse.write_fp_x_flux_zarr

Numba compiles the kernel on first use in each process. For benchmarks or a
distributed cluster, the optional warm-up can be run on every worker with
``client.run(warm_numba_fp_x_flux)``. It is not required for correctness.

.. autofunction:: openghg.analyse.warm_numba_fp_x_flux
