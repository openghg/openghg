"""Prepare particle-weighted boundary contributions on native footprint curtains."""

from typing import cast

import dask.array as da
import numpy as np
import pandas as pd
import xarray as xr
from pint.errors import PintError

from openghg.analyse._alignment import reindex_time_to_target
from openghg.util import check_lifetime_monthly, relative_time_offset, species_lifetime, time_offset

__all__ = ["baseline_sensitivities"]


def _age_hours(fp: xr.Dataset, direction: str, require_units: bool = False) -> xr.DataArray:
    """Read one curtain's stored ages as dequantified values in hours.

    Args:
        fp: Footprint releases containing ``mean_age_particles_{direction}`` and
            the corresponding ``particle_locations_{direction}`` weights.
        direction: Curtain suffix: ``"n"``, ``"e"``, ``"s"`` or ``"w"``.
        require_units: Require duration units convertible to hours for mean-age
            selection. If False, retain legacy unitless ages interpreted as hours
            by the release-time lifetime calculation.

    Returns:
        Ages with their native dimensions and coordinates. In strict mode, an
        entirely zero-weight curtain may instead return zero placeholders when
        its age array or units are unusable. Checking that exception computes a
        scalar reduction of the weights, including for chunked inputs.

    Raises:
        ValueError: If required ages are missing or have unusable duration units
            and the strict-mode inactive-curtain exception does not apply.
    """
    name = f"mean_age_particles_{direction}"
    try:
        if name not in fp:
            raise ValueError(f"Missing {name}; species-specific footprints with particle ages are required.")
        age = fp[name]
        if age.pint.units is None and not age.attrs.get("units"):
            if require_units:
                raise ValueError(f"{name} must declare duration units, such as hours.")
            return age
        return cast(xr.DataArray, age.pint.quantify().pint.to("hour").pint.dequantify())
    except (PintError, ValueError, TypeError) as exc:
        weights = fp[f"particle_locations_{direction}"]
        if require_units and not bool((weights != 0).any().compute()):
            return xr.zeros_like(weights, dtype=float)
        raise ValueError(f"{name} must have duration units convertible to hours.") from exc


def _calendar_year(times: np.ndarray, year: int) -> np.ndarray:
    """Map calendar dates to a climatology year while preserving time of day.

    Args:
        times: Timestamps as integer epoch nanoseconds or datetime64 values, with
            any target-cell shape.
        year: Reference year for the supplied climatology.

    Returns:
        Integer epoch nanoseconds with the input shape. Month, day and time of day
        are retained, except that February 29 becomes February 28 in a non-leap
        reference year, matching the existing release-time alignment policy.
    """
    dates = times.astype("datetime64[ns]")
    days = dates.astype("datetime64[D]")
    months = dates.astype("datetime64[M]")
    mapped_months = np.datetime64(f"{year}-01", "M") + months.astype("int64") % 12
    month_days = ((mapped_months + 1).astype("datetime64[D]") - mapped_months).astype(int)
    # Clamp the day within its reference month, then restore the subdaily offset.
    day_index = np.minimum((days - months).astype(int), month_days - 1)
    return cast(
        np.ndarray,
        (mapped_months.astype("datetime64[ns]") + day_index.astype("timedelta64[D]") + (dates - days)).astype(
            "int64"
        ),
    )


def _boundary_intervals(
    bc: xr.Dataset, fp: xr.Dataset, period: str | None, allow_climatology: bool
) -> tuple[np.ndarray, np.ndarray, np.ndarray, int | None]:
    """Resolve native temporal support and map sorted intervals to stored BC rows.

    Args:
        bc: Boundary fields with a one-dimensional, unique ``time`` coordinate.
            Explicit bounds named by ``time.attrs["bounds"]`` must have shape
            ``(time, 2)`` and take precedence over the supplied period.
        fp: Native footprint release times, used to decide climatology eligibility.
        period: Width of start-labelled intervals when explicit bounds are absent.
            Calendar periods such as ``"monthly"`` use calendar offsets; interval
            widths are never inferred from gaps between stored timestamps.
        allow_climatology: Allow a single source year whose timestamp range does
            not overlap the release range to serve as a reference climatology.

    Returns:
        Four values: sorted interval starts and exclusive ends as one-dimensional
        integer epoch-nanosecond arrays; original BC storage-row indices for those
        intervals; and the optional climatology reference year. Intervals crossing
        the reference-year seam also appear in the preceding cycle and may share
        a storage row. Only time coordinates and bounds are read here.

    Raises:
        ValueError: If times are empty, missing or duplicated, bounds are malformed,
            a required period is absent or invalid, or interval support is
            nonfinite, nonpositive or overlapping. Internal gaps are allowed and
            checked against individual target cells during lookup.
    """
    source_times = pd.DatetimeIndex(bc.time.values)
    if source_times.empty or source_times.hasnans or not source_times.is_unique:
        raise ValueError("Boundary times must be nonempty, finite and unique.")

    bounds_name = bc.time.attrs.get("bounds")
    if bounds_name is not None:
        if bounds_name not in bc:
            raise ValueError(f"Boundary time bounds {bounds_name!r} are missing.")
        bounds = bc[bounds_name].transpose("time", ...).values.astype("datetime64[ns]")
        if bounds.shape != (len(source_times), 2):
            raise ValueError("Boundary time bounds must have shape (time, 2).")
        starts, ends = bounds[:, 0], bounds[:, 1]
    else:
        if period is None or period == "varies":
            raise ValueError("mean_age requires explicit boundary time bounds or bc_time_period.")
        try:
            offset = relative_time_offset(period=period)
            starts = source_times.to_numpy(dtype="datetime64[ns]")
            ends = (source_times + offset).to_numpy(dtype="datetime64[ns]")
        except (ValueError, TypeError, OverflowError) as exc:
            raise ValueError(f"Invalid boundary interval period {period!r}.") from exc

    storage_rows = np.arange(len(source_times))
    reference_year = None
    release_times = pd.DatetimeIndex(fp.time.values)
    overlap = source_times.min() <= release_times.max() and release_times.min() <= source_times.max()
    if allow_climatology and source_times.year.nunique() == 1 and not overlap:
        reference_year = int(source_times.year[0])
        # Retain the preceding cycle only for intervals crossing the end of the reference year.
        wraps = ends > np.datetime64(f"{reference_year + 1}-01-01", "ns")
        starts = np.concatenate([starts, (pd.DatetimeIndex(starts[wraps]) - pd.DateOffset(years=1)).values])
        ends = np.concatenate([ends, (pd.DatetimeIndex(ends[wraps]) - pd.DateOffset(years=1)).values])
        storage_rows = np.concatenate([storage_rows, storage_rows[wraps]])

    # Sort temporal support without assuming the concentration arrays use that storage order.
    order = np.argsort(starts)
    starts, ends, storage_rows = starts[order], ends[order], storage_rows[order]
    if (
        np.isnat(starts).any()
        or np.isnat(ends).any()
        or (ends <= starts).any()
        or (ends[:-1] > starts[1:]).any()
    ):
        raise ValueError("Boundary intervals must have finite, positive, nonoverlapping support.")
    return starts.astype("int64"), ends.astype("int64"), storage_rows, reference_year


def _lagged_rows(
    age: np.ndarray,
    weight: np.ndarray,
    release: np.ndarray,
    *,
    starts: np.ndarray,
    ends: np.ndarray,
    reference_year: int | None,
) -> np.ndarray:
    """Locate each active cell's lagged time within sorted native BC intervals.

    Args:
        age: Stored per-cell mean ages in numeric hours for one curtain block.
        weight: Finite, nonnegative, dimensionless particle weights, broadcastable
            with ages. Zero-weight cells need no usable age or temporal support.
        release: Native datetime64 release times, broadcastable to the cell block.
        starts: Sorted one-dimensional interval starts as integer epoch nanoseconds.
        ends: Corresponding exclusive interval ends in the same units.
        reference_year: Optional climatology year. Each crossing time is mapped
            after age subtraction, including the existing February 29 clamp.

    Returns:
        Integer positions in the sorted interval arrays with the broadcast target
        shape. These are interval indices, not original BC storage-row indices.
        Inactive cells receive index zero as a placeholder for subsequent masking.
        No target-cell by source-time array is constructed.

    Raises:
        ValueError: If weights are invalid, active ages are nonfinite or negative,
            age subtraction exceeds the nanosecond datetime range, or an active
            crossing time lacks interval support, including within an internal gap.
    """
    if (~np.isfinite(weight) | (weight < 0)).any():
        raise ValueError("Particle weights must be finite and nonnegative.")
    active = weight > 0
    if (active & (~np.isfinite(age) | (age < 0))).any():
        raise ValueError("Positive-weight particle ages must be finite and nonnegative.")

    # Preserve fractional hours down to the precision of the nanosecond time coordinate.
    hours = np.where(active, age, 0)
    nanoseconds_per_hour = 3.6e12
    if (hours >= np.iinfo(np.int64).max / nanoseconds_per_hour).any():
        raise ValueError("Particle ages exceed the supported datetime range.")
    release_ns = release.astype("datetime64[ns]").astype("int64")
    targets = release_ns - np.rint(hours * nanoseconds_per_hour).astype("int64")
    # Integer underflow would make a positive age select a time after its release.
    if (active & (targets > release_ns)).any():
        raise ValueError("Particle ages exceed the supported datetime range.")
    if reference_year is not None:
        targets = _calendar_year(targets, reference_year)

    # Inactive targets use a safe placeholder; only positive weights require coverage.
    targets = np.where(active, targets, starts[0])
    positions = np.searchsorted(starts, targets, side="right") - 1
    interval_indices = np.maximum(positions, 0)
    unsupported = active & ((positions < 0) | (targets >= ends[interval_indices]))
    if unsupported.any():
        missing = np.asarray(targets)[unsupported].flat[0].astype("datetime64[ns]")
        raise ValueError(
            f"Boundary conditions do not cover positive-weight target {missing}; "
            "supply sufficient earlier history and complete native intervals (including gaps and final expiry)."
        )
    return cast(np.ndarray, interval_indices)


def _check_concentrations(values: np.ndarray, weights: np.ndarray) -> np.ndarray:
    """Validate selected concentrations and mask inactive cells before multiplication.

    Args:
        values: Selected numeric mole fractions in the source field's units.
        weights: Broadcast-compatible dimensionless particle weights.

    Returns:
        Selected concentrations at positive-weight cells and exactly zero
        elsewhere, so inactive NaNs cannot propagate through multiplication.

    Raises:
        ValueError: If a selected active concentration is nonfinite.
    """
    if ((weights > 0) & ~np.isfinite(values)).any():
        raise ValueError("Boundary concentrations must be finite at positive-weight targets.")
    return np.where(weights > 0, values, 0)


def _mean_age_boundary(
    bc: xr.Dataset, fp: xr.Dataset, period: str | None, allow_climatology: bool
) -> xr.Dataset:
    """Select a boundary concentration at each native release and curtain cell.

    Args:
        bc: ``vmr_n/s(time, height, lon)`` and ``vmr_e/w(time, height, lat)``
            fields with mole-fraction units and declared native temporal support.
            Spatial coordinates are aligned to the footprints using the existing
            unit-aware forward-fill policy; source time is retained for lookup.
        fp: Native releases containing dimensionless ``particle_locations_*``
            weights. Positive-weight cells require matching ``mean_age_particles_*``
            arrays with duration units convertible to hours; inactive curtains may
            omit usable ages. Curtain dimension order may vary.
        period: Width of start-labelled intervals, used only when explicit CF
            time bounds are absent. This declares lookup support, not whether
            a supplied concentration represents an interval mean or an instant.
        allow_climatology: Allow eligible single-reference-year fields to be
            calendar-mapped after subtracting cell ages from release times.

    Returns:
        Unweighted ``vmr_*`` arrays in the supplied concentration units, retaining
        each curtain's native release and spatial coordinates and dimension order.
        Scalar reductions stream ages and weights to bound the selected source
        interval range before gathering. Concentration reads stay lazy for chunked
        inputs; entirely inactive curtains read no concentrations. Inputs are
        preserved, and no release-by-cell-by-source-time intermediate is needed.

    Raises:
        ValueError: If temporal support, required ages, duration units, matching
            curtain dimensions or particle weights are invalid. Nonfinite active
            concentrations raise when their selected values are computed.
    """
    starts, ends, storage_rows, reference_year = _boundary_intervals(bc, fp, period, allow_climatology)

    # Give source time a distinct name so spatial reindexing cannot crop the needed history.
    fields = bc[[f"vmr_{d}" for d in "nesw"]].rename(time="bc_time")
    fields = (
        fields.pint.quantify()
        .pint.reindex_like(xr.Dataset(coords=fp.coords).pint.quantify(), "ffill")
        .pint.dequantify()
    )
    selected = {}
    for direction in "nesw":
        weight = fp[f"particle_locations_{direction}"].pint.quantify()
        if weight.pint.units is not None:
            weight = weight.pint.to("dimensionless")
        weight = weight.pint.dequantify()
        age = _age_hours(fp, direction, require_units=True)
        if set(age.dims) != set(weight.dims):
            raise ValueError(
                f"Particle ages and weights for {direction} must have the same curtain dimensions."
            )

        interval_indices = xr.apply_ufunc(
            _lagged_rows,
            age,
            weight,
            fp.time,
            kwargs={"starts": starts, "ends": ends, "reference_year": reference_year},
            dask="parallelized",
            output_dtypes=[np.int64],
        )
        spatial_dims = [d for d in weight.dims if d != "time"]
        spatial_shape = tuple(weight.sizes[d] for d in spatial_dims)
        cell_count = int(np.prod(spatial_shape))
        cell_indices = xr.DataArray(np.arange(cell_count).reshape(spatial_shape), dims=spatial_dims)

        # Bound source reads before Dask pairs source chunks with lazy index chunks.
        interval_limits = xr.Dataset(
            {
                "first": interval_indices.where(weight > 0, len(starts)).min(),
                "last": interval_indices.where(weight > 0, 0).max(),
            }
        ).compute()
        first_interval, last_interval = int(interval_limits["first"]), int(interval_limits["last"])
        if first_interval > last_interval:
            concentration = xr.zeros_like(weight)
            concentration.attrs = fields[f"vmr_{direction}"].attrs
            selected[f"vmr_{direction}"] = concentration
            continue

        # Reset inactive placeholders to this window so they cannot pull in older history.
        interval_indices = interval_indices.where(weight > 0, first_interval) - first_interval
        # Flatten (interval, cell) as interval * cell_count + cell, preserving cell identity.
        flat_indices = (interval_indices * cell_count + cell_indices).transpose("time", *spatial_dims)
        source = fields[f"vmr_{direction}"].isel(bc_time=storage_rows[first_interval : last_interval + 1])
        source = source.transpose("bc_time", *spatial_dims)
        if source.chunks is not None or flat_indices.chunks is not None:
            values = da.take(da.asarray(source.data).ravel(), da.asarray(flat_indices.data).ravel())
        else:
            values = np.take(source.values.ravel(), flat_indices.values.ravel())

        # Restore the footprint layout before masking zero weights or multiplying contributions.
        concentration = xr.DataArray(
            values.reshape(flat_indices.shape),
            dims=flat_indices.dims,
            coords=flat_indices.coords,
            attrs=source.attrs,
        ).transpose(*weight.dims)
        concentration = xr.apply_ufunc(
            _check_concentrations,
            concentration,
            weight,
            dask="parallelized",
            keep_attrs=True,
            output_dtypes=[source.dtype],
        )
        selected[f"vmr_{direction}"] = concentration
    return xr.Dataset(selected)


def baseline_sensitivities(
    bc: xr.Dataset,
    fp: xr.Dataset,
    species: str | None = None,
    allow_climatology: bool = True,
    *,
    bc_time_method: str = "release_time",
    bc_time_period: str | None = None,
) -> xr.Dataset:
    """Compute particle-weighted concentration contributions from NESW boundary curtains.

    Args:
        bc: Boundary concentrations ``vmr_n/s(time, height, lon)`` and
            ``vmr_e/w(time, height, lat)`` with declared mole-fraction units.
        fp: Native footprint releases with matching ``particle_locations_*`` curtain
            weights. Dimension order may vary. Positive-weight cells in mean-age mode
            require matching ``mean_age_particles_*`` arrays with duration units convertible to hours.
        species: Optional species used for the existing lifetime-loss factor.
        allow_climatology: Allow single-reference-year data outside the release range
            to be calendar-mapped. Lagging precedes mapping, with February 29 clamped
            to February 28 when necessary. Missing native intervals still fail in mean-age mode.
        bc_time_method: ``"release_time"`` preserves the existing time selection.
            ``"mean_age"`` selects at each release time minus its fractional stored
            cell age, without additional age rounding or concentration interpolation.
            The supplied fields must represent start-labelled native intervals or have
            explicit time bounds; the lag choice does not infer physical field semantics.
        bc_time_period: Native interval width, such as ``"3h"``, ``"1D"`` or
            ``"monthly"``. Used only in mean-age mode, with ``bc.attrs["time_period"]``
            as fallback. CF ``time.attrs["bounds"]`` takes precedence. Intervals include
            their start and exclude their end. Ambiguous support, internal gaps and
            extrapolation are rejected at positive-weight targets.

    Returns:
        Unreduced float32 ``bc_n/s`` and ``bc_e/w`` contributions, retaining the native
        release-time coordinate, each curtain's spatial dimensions and concentration
        units. Includes the existing lifetime loss once. Zero-weight cells remain zero
        in mean-age mode without requiring valid ages or concentrations there.
        Inputs are preserved. In mean-age mode, scalar reductions of ages and
        weights bound source history reads; concentration gathering and outputs
        remain lazy when inputs are chunked. Dataset attributes record the method
        and the applicable interval policy, bounds or period.

    Raises:
        ValueError: If the method, required ages, duration units, weights or native
            temporal support are invalid, or selected active concentrations are nonfinite.
            Supply earlier BC history before lagging.
    """
    if bc_time_method not in ("release_time", "mean_age"):
        raise ValueError("bc_time_method must be 'release_time' or 'mean_age'.")

    bc = bc.copy(deep=False)
    # Missing coordinate units inherit footprint units; their numeric scales are assumed aligned.
    for dim in bc.dims:
        if dim in fp.dims and "units" not in bc[dim].attrs:
            bc[dim].attrs["units"] = fp[dim].attrs.get("units")

    period = bc_time_period if bc_time_period is not None else bc.attrs.get("time_period")
    bounds_name = bc.time.attrs.get("bounds")
    if bc_time_method == "mean_age":
        bc = _mean_age_boundary(bc, fp, period, allow_climatology)
    else:
        bc = reindex_time_to_target(bc, fp, allow_climatology=allow_climatology)

    native_fp = fp
    fp = fp[[f"particle_locations_{d}" for d in "nesw"]].pint.quantify()
    bc = bc.pint.quantify().pint.reindex_like(fp, "ffill")

    # Align time chunks after filling, matching the footprint release blocks.
    fp_time_chunks = fp.particle_locations_n.chunksizes.get("time")
    if fp_time_chunks is not None:
        bc = bc.chunk({"time": fp_time_chunks[0]})

    lifetime_value = species_lifetime(species)
    lifetime_hrs: float | xr.DataArray | None = None
    if check_lifetime_monthly(lifetime_value):
        monthly_hours = [
            time_offset(period=lt).total_seconds() / 3600 for lt in cast(list[str], lifetime_value)
        ]
        # Monthly lifetimes follow each receptor release month, independently of the lagged BC time.
        lifetime_hrs = xr.DataArray(
            [monthly_hours[month - 1] for month in fp.time.dt.month.values],
            dims="time",
            coords={"time": fp.time},
        )
    elif lifetime_value is not None:
        lifetime_hrs = time_offset(period=cast(str, lifetime_value)).total_seconds() / 3600

    sensitivities = {}
    for direction in "nesw":
        weight = fp[f"particle_locations_{direction}"]
        loss: xr.DataArray | float = 1.0
        if lifetime_hrs is not None:
            # Concentration lagging is additional to this single age-dependent loss factor.
            age = _age_hours(native_fp, direction, require_units=bc_time_method == "mean_age")
            if bc_time_method == "mean_age":
                age = age.where(native_fp[f"particle_locations_{direction}"] > 0, 0)
            loss = cast(xr.DataArray, np.exp(-age / lifetime_hrs))
            # The exponential is dimensionless; quantify coordinate units for alignment with fp.
            loss.attrs = {}
            loss = loss.pint.quantify()
        sensitivities[f"bc_{direction}"] = weight * bc[f"vmr_{direction}"] * loss

    # Store units as attributes to avoid Pint quantities in downstream Dask computations.
    result = cast(xr.Dataset, xr.Dataset(sensitivities).pint.dequantify())
    # Retain the existing float32 output precision.
    result = result.astype({f"bc_{d}": "float32" for d in "nesw"})

    result.attrs["bc_time_method"] = bc_time_method
    if bc_time_method == "mean_age":
        result.attrs["bc_time_policy"] = "interval_start"
        if bounds_name is not None:
            result.attrs["bc_time_bounds"] = bounds_name
        elif period is not None:
            result.attrs["bc_time_period"] = period
    return result
