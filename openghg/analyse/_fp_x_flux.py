"""Lazy, source-resolved footprint-times-flux calculations."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
import json
import os
import shutil
from typing import Any, ParamSpec, TypeVar, cast
from uuid import uuid4

import dask.array as da
import numpy as np
import pandas as pd
import xarray as xr

try:
    from numba import njit  # type: ignore[import-not-found]
except ImportError:  # pragma: no cover - exercised in base installs without the optional extra
    njit = None  # type: ignore[assignment]


SPATIAL_DIMS = ("lat", "lon")
REQUIRED_OUTPUT_DIMS = ("source", "lat", "lon", "time")
FP_X_FLUX_OPERATOR = "openghg.analyse.fp_x_flux_keep_space"
FP_X_FLUX_OPERATOR_VERSION = 2
FLUX_TIME_ALIGNMENT = "interval_start"
DEFAULT_IRREGULAR_TIME_CHUNK = 32

_P = ParamSpec("_P")
_R = TypeVar("_R")


def _numba_kernel(function: Callable[_P, _R]) -> Callable[_P, _R]:
    """Decorate a kernel when the optional Numba dependency is installed."""
    if njit is None:
        return function
    return cast(Callable[_P, _R], njit(cache=True)(function))


def _require_numba() -> None:
    if njit is None:
        raise ImportError(
            "fp_x_flux_keep_space requires the optional Numba dependency. "
            "Install OpenGHG with `pip install 'openghg[fp-x-flux]'`."
        )


def _as_flux_data_array(flux: xr.DataArray | xr.Dataset) -> xr.DataArray:
    if isinstance(flux, xr.Dataset):
        if "flux" not in flux:
            raise ValueError("Flux Dataset must contain a 'flux' variable.")
        return cast(xr.DataArray, flux["flux"])
    return flux


def _split_footprint(fp: xr.DataArray | xr.Dataset) -> xr.Dataset:
    if isinstance(fp, xr.Dataset):
        missing = {"fp_time_resolved", "fp_residual"} - set(fp.data_vars)
        if missing:
            raise ValueError(f"Footprint Dataset is missing variables: {sorted(missing)}")
        return fp[["fp_time_resolved", "fp_residual"]]

    if "H_back" not in fp.dims:
        raise ValueError("Footprint DataArray must include an 'H_back' dimension.")
    if fp.sizes["H_back"] < 2:
        raise ValueError("Footprint DataArray must contain at least one resolved lag and one residual slice.")
    return xr.Dataset(
        {
            "fp_time_resolved": fp.isel(H_back=slice(None, -1)),
            "fp_residual": fp.isel(H_back=-1, drop=True),
        }
    )


def _reindex_space(data: xr.DataArray, footprint: xr.Dataset) -> xr.DataArray:
    indexers = {dim: footprint.coords[dim] for dim in SPATIAL_DIMS}
    if all(data.coords[dim].identical(indexers[dim]) for dim in SPATIAL_DIMS):
        return data
    try:
        return data.interp(indexers)
    except (TypeError, ValueError):
        return data.reindex(indexers, method="nearest")


def _prepare_inputs(
    fp: xr.DataArray | xr.Dataset,
    flux: xr.DataArray | xr.Dataset,
    *,
    cast_float32: bool,
    fillna_zero: bool,
) -> tuple[xr.Dataset, xr.DataArray, xr.DataArray, xr.DataArray]:
    fp_ds = _split_footprint(fp)
    flux_da = _as_flux_data_array(flux)

    required_fp = {"time", "lat", "lon", "H_back"}
    missing_fp = required_fp - set(fp_ds["fp_time_resolved"].dims)
    if missing_fp:
        raise ValueError(f"Time-resolved footprint is missing dimensions: {sorted(missing_fp)}")
    required_residual = {"time", "lat", "lon"}
    missing_residual = required_residual - set(fp_ds["fp_residual"].dims)
    if missing_residual:
        raise ValueError(f"Residual footprint is missing dimensions: {sorted(missing_residual)}")
    missing_flux = {"time", "lat", "lon"} - set(flux_da.dims)
    if missing_flux:
        raise ValueError(f"Flux is missing dimensions: {sorted(missing_flux)}")
    if fp_ds.sizes["time"] == 0:
        raise ValueError("Footprint time coordinate must not be empty.")
    if flux_da.sizes["time"] == 0:
        raise ValueError("Flux time coordinate must not be empty.")

    flux_da = _reindex_space(flux_da, fp_ds)
    if cast_float32:
        fp_ds = fp_ds.astype(np.float32)
        flux_da = flux_da.astype(np.float32)
    if fillna_zero:
        fp_ds = fp_ds.fillna(0.0)
        flux_da = flux_da.fillna(0.0)

    return (
        fp_ds,
        cast(xr.DataArray, fp_ds["fp_time_resolved"]),
        cast(xr.DataArray, fp_ds["fp_residual"]),
        flux_da,
    )


def _hourly_lags(fp_time_resolved: xr.DataArray) -> np.ndarray:
    lag_coord = fp_time_resolved["H_back"]
    units = str(lag_coord.attrs.get("units", "hours")).lower()
    if units not in {"h", "hr", "hour", "hours"}:
        raise ValueError(f"H_back must be expressed in hours, found {units!r}.")
    values = np.asarray(lag_coord.values, dtype=float)
    rounded = np.rint(values)
    if not np.allclose(values, rounded) or np.any(rounded < 0):
        raise ValueError("H_back values must be non-negative whole hours.")
    return cast(np.ndarray, rounded.astype(np.int64))


def _regular_time_step_hours(time: xr.DataArray, *, label: str) -> int:
    if time.size < 2:
        return 1
    diffs_ns = np.diff(time.values.astype("datetime64[ns]")).astype("timedelta64[ns]").astype(np.int64)
    hour_ns = np.timedelta64(1, "h").astype("timedelta64[ns]").astype(np.int64)
    if np.any(diffs_ns <= 0) or not np.all(diffs_ns == diffs_ns[0]) or diffs_ns[0] % hour_ns:
        raise ValueError(f"{label} time coordinate must be regular with a whole-hour frequency.")
    return int(diffs_ns[0] // hour_ns)


def _validate_interval_start_time(time: xr.DataArray, *, label: str) -> int:
    time_label = str(time.attrs.get("label", "left")).lower()
    if time_label not in {"left", "start", "beginning"}:
        raise ValueError(
            f"{label} timestamps must label the start of each averaging interval; "
            f"found label={time_label!r}."
        )
    return _regular_time_step_hours(time, label=label)


def _interval_start_indices(
    flux_time: xr.DataArray,
    target_times: np.ndarray,
    *,
    step_hours: int,
) -> np.ndarray:
    """Map targets into half-open, left-labelled flux intervals."""
    flux_ns = np.asarray(flux_time.values, dtype="datetime64[ns]").astype(np.int64)
    if flux_ns.size == 0:
        raise ValueError("Flux time coordinate must not be empty.")
    targets = np.asarray(target_times, dtype="datetime64[ns]")
    target_ns = targets.astype(np.int64)
    indices = np.searchsorted(flux_ns, target_ns, side="right") - 1
    step_ns = np.timedelta64(step_hours, "h").astype("timedelta64[ns]").astype(np.int64)
    clipped = np.clip(indices, 0, max(0, flux_ns.size - 1))
    valid = (indices >= 0) & (indices < flux_ns.size) & (target_ns < flux_ns[clipped] + step_ns)
    if not np.all(valid):
        invalid = targets[~valid]
        raise ValueError(
            f"Flux time coverage does not contain {invalid.size} target timestamp(s) within "
            f"left-labelled {step_hours}-hour interval(s); first invalid target is {invalid.flat[0]}."
        )
    return cast(np.ndarray, indices.astype(np.int64))


def align_flux_to_time_targets(
    flux: xr.DataArray | xr.Dataset,
    target_times: xr.DataArray | Sequence[Any],
) -> xr.DataArray:
    """Align flux to target times using interval-start membership.

    Flux timestamps are interpreted as the starts of regular, half-open
    averaging intervals. A target at ``t`` receives the flux whose interval is
    ``[flux_time, flux_time + cadence)`` and contains ``t``. Targets outside
    the represented intervals raise ``ValueError``; values are never matched
    by nearest-neighbour or extrapolated across a missing interval.

    Args:
        flux: Flux DataArray, or a Dataset containing ``flux``.
        target_times: One-dimensional timestamps to align to.

    Returns:
        Flux values with their time dimension replaced by the exact target
        timestamps, preserving target order and duplicates.
    """
    flux_da = _as_flux_data_array(flux)
    if "time" not in flux_da.dims:
        raise ValueError("Flux is missing the 'time' dimension.")
    values = target_times.values if isinstance(target_times, xr.DataArray) else target_times
    targets = np.asarray(values, dtype="datetime64[ns]")
    if targets.ndim != 1:
        raise ValueError("target_times must be one-dimensional.")
    step_hours = _validate_interval_start_time(flux_da["time"], label="Flux")
    indices = _interval_start_indices(flux_da["time"], targets, step_hours=step_hours)
    indexer = xr.DataArray(indices, dims="time")
    aligned = flux_da.isel(time=indexer).assign_coords(time=targets)
    aligned.attrs = dict(flux_da.attrs)
    return aligned


def _h_back_window_hours(fp_time_resolved: xr.DataArray) -> int:
    lags = np.sort(_hourly_lags(fp_time_resolved))
    if lags.size < 2:
        return int(lags.max(initial=0)) + 1
    lag_step = int(np.gcd.reduce(np.diff(lags)))
    return int(lags.max()) + max(1, lag_step)


def _forward_fill_flux_hourly(flux: xr.DataArray, *, step_hours: int) -> xr.DataArray:
    if step_hours == 1:
        return flux
    end = flux["time"].values[-1] + np.timedelta64(step_hours, "h")
    hourly_time = pd.date_range(flux["time"].values[0], end, freq="h", inclusive="left")
    result = flux.reindex(time=hourly_time, method="ffill")
    result.attrs = dict(flux.attrs)
    return result


def _padded_flux_bounds(fp_time_resolved: xr.DataArray) -> tuple[np.datetime64, np.datetime64]:
    max_lag = int(_hourly_lags(fp_time_resolved).max(initial=0))
    start = fp_time_resolved["time"].values[0] - np.timedelta64(max_lag, "h")
    end = fp_time_resolved["time"].values[-1]
    return start, end


def _low_frequency_flux(flux: xr.DataArray, fp_ds: xr.Dataset) -> xr.DataArray:
    # Include complete calendar months so boundary releases use a full-month
    # mean without reading unrelated years from a long flux record.
    release_index = pd.DatetimeIndex(fp_ds["time"].values)
    first_month = release_index.min().to_period("M").start_time
    month_after_last = (release_index.max().to_period("M") + 1).start_time
    final_instant = month_after_last.to_datetime64() - np.timedelta64(1, "ns")
    monthly = flux.sel(time=slice(first_month, final_instant)).resample(time="1MS").mean()
    return monthly.reindex(time=fp_ds["time"], method="ffill").astype(np.float32)


@_numba_kernel
def _resolved_keep_space_block(
    fp_block: np.ndarray,
    flux_block: np.ndarray,
    lag_indices: np.ndarray,
) -> np.ndarray:
    nt, ny, nx, nh = fp_block.shape
    ns = flux_block.shape[3]
    out = np.empty((nt, ny, nx, ns), dtype=fp_block.dtype)
    left_halo = flux_block.shape[0] - nt
    for t in range(nt):
        for y in range(ny):
            for x in range(nx):
                for source in range(ns):
                    value = 0.0
                    for lag in range(nh):
                        flux_t = t + left_halo - lag_indices[lag]
                        if 0 <= flux_t < flux_block.shape[0]:
                            value += fp_block[t, y, x, lag] * flux_block[flux_t, y, x, source]
                    out[t, y, x, source] = value
    return out


@_numba_kernel
def _resolved_keep_space_indexed_block(
    fp_block: np.ndarray,
    flux_block: np.ndarray,
    flux_indices: np.ndarray,
) -> np.ndarray:
    """Apply an observation-block-specific flux index map."""
    nt, ny, nx, nh = fp_block.shape
    ns = flux_block.shape[3]
    out = np.empty((nt, ny, nx, ns), dtype=fp_block.dtype)
    for t in range(nt):
        for y in range(ny):
            for x in range(nx):
                for source in range(ns):
                    value = 0.0
                    for lag in range(nh):
                        value += fp_block[t, y, x, lag] * flux_block[flux_indices[t, lag], y, x, source]
                    out[t, y, x, source] = value
    return out


def _chunks_with_minimum_tail(length: int, target: int | None, minimum: int) -> tuple[int, ...]:
    if target is None or target >= length:
        return (length,)
    if target <= 0:
        raise ValueError("Chunk sizes must be positive.")
    chunks = [target] * (length // target)
    remainder = length % target
    if remainder:
        chunks.append(remainder)
    if len(chunks) > 1 and chunks[-1] < minimum:
        tail = chunks.pop()
        chunks[-1] += tail
    return tuple(chunks)


def _chunk_inputs(
    fp_time_resolved: xr.DataArray,
    fp_residual: xr.DataArray,
    flux: xr.DataArray,
    *,
    time_chunk: int | None,
    lat_chunk: int | None,
    lon_chunk: int | None,
    source_chunk: int | None,
    minimum_time_chunk: int,
) -> tuple[xr.DataArray, xr.DataArray, xr.DataArray]:
    time_chunks = _chunks_with_minimum_tail(
        fp_time_resolved.sizes["time"],
        time_chunk,
        minimum_time_chunk,
    )
    fp_chunks: dict[str, Any] = {
        "time": time_chunks,
        "lat": lat_chunk or fp_time_resolved.sizes["lat"],
        "lon": lon_chunk or fp_time_resolved.sizes["lon"],
        "H_back": fp_time_resolved.sizes["H_back"],
    }
    residual_chunks = {key: value for key, value in fp_chunks.items() if key != "H_back"}
    flux_chunks: dict[str, Any] = {
        "lat": fp_chunks["lat"],
        "lon": fp_chunks["lon"],
        "source": source_chunk or flux.sizes["source"],
    }
    return fp_time_resolved.chunk(fp_chunks), fp_residual.chunk(residual_chunks), flux.chunk(flux_chunks)


def _pad_footprint_left(
    fp_time_resolved: xr.DataArray,
    *,
    pad_hours: int,
) -> xr.DataArray:
    if pad_hours == 0:
        return fp_time_resolved
    original_chunks = tuple(int(value) for value in fp_time_resolved.chunksizes["time"])
    first_time = fp_time_resolved["time"].values[0]
    pad_times = first_time - np.arange(pad_hours, 0, -1, dtype="timedelta64[h]")
    padded = fp_time_resolved.pad(
        {"time": (pad_hours, 0)}, mode="constant", constant_values=0.0
    ).assign_coords(time=np.concatenate([pad_times, fp_time_resolved["time"].values]))
    return padded.chunk({"time": (original_chunks[0] + pad_hours,) + original_chunks[1:]})


def _flux_with_halo(flux: xr.DataArray, fp_time_resolved: xr.DataArray) -> xr.DataArray:
    lags = _hourly_lags(fp_time_resolved)
    max_lag = int(lags.max(initial=0))
    start, end = _padded_flux_bounds(fp_time_resolved)
    if flux["time"].values[0] > start or flux["time"].values[-1] < end:
        raise ValueError(
            "Flux time coverage must include the complete footprint lag halo " f"from {start} through {end}."
        )
    flux_pad = flux.sel(time=slice(start, end)).transpose("time", "lat", "lon", "source")

    fp_time_chunks = tuple(int(value) for value in fp_time_resolved.chunksizes["time"])
    chunks: dict[str, Any] = {
        "time": (fp_time_chunks[0] + max_lag,) + fp_time_chunks[1:],
        "lat": tuple(int(value) for value in fp_time_resolved.chunksizes["lat"]),
        "lon": tuple(int(value) for value in fp_time_resolved.chunksizes["lon"]),
    }
    if hasattr(flux.data, "chunks"):
        chunks["source"] = flux.data.chunks[flux.get_axis_num("source")]
    flux_pad = flux_pad.chunk(chunks)
    halo = da.map_overlap(
        lambda values: values,
        flux_pad.data,
        depth={0: (max_lag, 0)},
        boundary="none",
        trim=False,
        align_arrays=True,
        allow_rechunk=False,
        dtype=flux_pad.dtype,
        meta=np.array((), dtype=flux_pad.dtype),
    )
    return xr.DataArray(halo, dims=flux_pad.dims, coords=flux_pad.coords)


def _uses_regular_halo_kernel(fp_time: xr.DataArray, flux_time: xr.DataArray) -> bool:
    try:
        if _regular_time_step_hours(fp_time, label="Footprint") != 1:
            return False
    except ValueError:
        return False
    return bool(pd.DatetimeIndex(flux_time.values).get_indexer(pd.DatetimeIndex(fp_time.values)).min() >= 0)


def _indexed_flux_blocks(
    fp_time_resolved: xr.DataArray,
    flux: xr.DataArray,
    *,
    step_hours: int,
) -> tuple[da.Array, da.Array]:
    """Build one minimal contiguous flux slab and index map per footprint block."""
    lags = _hourly_lags(fp_time_resolved)
    release_times = np.asarray(fp_time_resolved["time"].values, dtype="datetime64[ns]")
    targets = release_times[:, None] - lags[None, :].astype("timedelta64[h]")
    global_indices = _interval_start_indices(flux["time"], targets, step_hours=step_hours)

    time_chunks = tuple(int(value) for value in fp_time_resolved.chunksizes["time"])
    slabs: list[da.Array] = []
    relative_indices = np.empty_like(global_indices)
    offset = 0
    for chunk in time_chunks:
        block_indices = global_indices[offset : offset + chunk]
        start = int(block_indices.min())
        stop = int(block_indices.max()) + 1
        slab = flux.data[start:stop].rechunk({0: -1})
        slabs.append(slab)
        relative_indices[offset : offset + chunk] = block_indices - start
        offset += chunk

    flux_blocks = da.concatenate(slabs, axis=0)
    index_blocks = da.from_array(
        relative_indices,
        chunks=(time_chunks, (fp_time_resolved.sizes["H_back"],)),
    )
    return flux_blocks, index_blocks


def _resolved_keep_space_indexed(
    fp_time_resolved: xr.DataArray,
    flux: xr.DataArray,
    *,
    step_hours: int,
) -> xr.DataArray:
    """Run the indexed Numba kernel for irregular footprint release times."""
    fp_time_resolved = fp_time_resolved.transpose("time", "lat", "lon", "H_back")
    flux = flux.transpose("time", "lat", "lon", "source")
    flux_blocks, index_blocks = _indexed_flux_blocks(
        fp_time_resolved,
        flux,
        step_hours=step_hours,
    )
    resolved_data = da.blockwise(
        _resolved_keep_space_indexed_block,
        "tyxs",
        fp_time_resolved.data,
        "tyxh",
        flux_blocks,
        "tyxs",
        index_blocks,
        "th",
        dtype=fp_time_resolved.dtype,
        concatenate=True,
        align_arrays=False,
        adjust_chunks={
            "t": fp_time_resolved.data.chunks[0],
            "y": fp_time_resolved.data.chunks[1],
            "x": fp_time_resolved.data.chunks[2],
            "s": flux_blocks.chunks[3],
        },
        meta=np.array((), dtype=fp_time_resolved.dtype),
    )
    return xr.DataArray(
        resolved_data,
        dims=("time", "lat", "lon", "source"),
        coords={
            "time": fp_time_resolved["time"],
            "lat": fp_time_resolved["lat"],
            "lon": fp_time_resolved["lon"],
            "source": flux["source"],
        },
    )


def _attach_source_coordinates(result: xr.DataArray, flux: xr.DataArray) -> xr.DataArray:
    coordinates: dict[str, xr.DataArray] = {}
    for name, coordinate in flux.coords.items():
        if not isinstance(name, str):
            continue
        if name in result.coords or "source" not in coordinate.dims:
            continue
        if set(coordinate.dims).issubset(result.dims):
            coordinates[name] = coordinate
    return result.assign_coords(coordinates)


def _normalise_time_selector(selector: xr.DataArray | Sequence[Any]) -> np.ndarray:
    values = selector.values if isinstance(selector, xr.DataArray) else selector
    index = pd.DatetimeIndex(pd.to_datetime(np.asarray(values).reshape(-1))).dropna().drop_duplicates()
    return cast(np.ndarray, index.to_numpy(dtype="datetime64[ns]"))


def fp_x_flux_keep_space(  # noqa: PLR0913
    footprint: xr.DataArray | xr.Dataset,
    flux: xr.DataArray | xr.Dataset,
    *,
    sources: Sequence[Any] | None = None,
    source_labels: Sequence[str] | None = None,
    time_selector: xr.DataArray | Sequence[Any] | None = None,
    time_chunk: int | None = None,
    lat_chunk: int | None = None,
    lon_chunk: int | None = None,
    source_chunk: int | None = None,
    cast_float32: bool = True,
    fillna_zero: bool = True,
) -> xr.DataArray:
    """Multiply time-resolved footprints by source-resolved flux, retaining space.

    The lazy result contains ``(source, lat, lon, time)`` and is suitable for
    later basis-function projection. The resolved term uses the flux interval
    containing each footprint release time minus ``H_back``; flux timestamps
    are interpreted as interval starts. The residual term uses monthly mean
    flux. Regular coarse flux is forward-filled to an hourly grid; flux coarser
    than the full ``H_back`` window uses its monthly mean for the resolved and
    residual terms. Inputs must include the full lag halo before the first
    footprint time. Irregular release times retain their exact order and values
    and use a chunk-indexed Numba kernel.

    If ``time_selector`` is supplied, it is applied only after the full lazy
    operator graph has been constructed. This preserves the lag calculation
    while allowing callers to retain only actual observation timestamps.

    Args:
        footprint: Dataset containing ``fp_time_resolved`` and ``fp_residual``,
            or a legacy ``H_back`` DataArray whose final lag is the residual.
        flux: Flux DataArray, or Dataset containing ``flux``. A missing
            ``source`` dimension is treated as a single source.
        sources: Optional source-coordinate values to select.
        source_labels: Optional replacement labels for the selected sources.
        time_selector: Observation timestamps to retain after calculation.
        time_chunk: Requested compute and output time chunk size. Irregular
            release times default to compute chunks of 32 observations; the
            regular kernel retains its existing full-time default.
        lat_chunk: Requested latitude chunk size; defaults to the full grid.
        lon_chunk: Requested longitude chunk size; defaults to the full grid.
        source_chunk: Requested source chunk size; defaults to all sources.
        cast_float32: Cast inputs and output to float32.
        fillna_zero: Replace missing footprint and flux values with zero.

    Returns:
        A lazy, dimensionless DataArray ordered as ``source, lat, lon, time``.
    """
    _require_numba()
    fp_ds, fp_time_resolved, fp_residual, flux_da = _prepare_inputs(
        footprint, flux, cast_float32=cast_float32, fillna_zero=fillna_zero
    )
    if "source" not in flux_da.dims:
        if sources is not None:
            raise ValueError("sources cannot be used when flux has no 'source' dimension.")
        if source_labels is not None and len(source_labels) != 1:
            raise ValueError("source_labels must contain exactly one label for single-source flux.")
        source_label = (
            source_labels[0]
            if source_labels is not None
            else str(flux_da.attrs.get("source") or flux_da.name or "source")
        )
        flux_da = flux_da.expand_dims(source=[source_label])
        source_labels = None
    elif sources is not None:
        flux_da = flux_da.sel(source=list(sources))
    if source_labels is not None:
        if len(source_labels) != flux_da.sizes["source"]:
            raise ValueError("source_labels must contain one label per selected source.")
        flux_da = flux_da.assign_coords(source=list(source_labels))
    flux_step_hours = _validate_interval_start_time(flux_da["time"], label="Flux")
    low_frequency_flux = _low_frequency_flux(flux_da, fp_ds)
    flux_metadata = flux_da
    restore_time_order: np.ndarray | None = None
    compute_time_chunk = time_chunk or fp_time_resolved.sizes["time"]

    if flux_step_hours > _h_back_window_hours(fp_time_resolved):
        kernel_name = "low_frequency_keep_space"
        fp_time_resolved, fp_residual, flux_da = _chunk_inputs(
            fp_time_resolved,
            fp_residual,
            flux_da,
            time_chunk=time_chunk,
            lat_chunk=lat_chunk,
            lon_chunk=lon_chunk,
            source_chunk=source_chunk,
            minimum_time_chunk=1,
        )
        resolved = fp_time_resolved.sum("H_back") * low_frequency_flux
    else:
        flux_da = _forward_fill_flux_hourly(flux_da, step_hours=flux_step_hours)
        aligned_flux_step_hours = 1
        use_regular_kernel = _uses_regular_halo_kernel(
            fp_time_resolved["time"],
            flux_da["time"],
        )
        if not use_regular_kernel:
            compute_time_chunk = time_chunk or min(
                DEFAULT_IRREGULAR_TIME_CHUNK,
                fp_time_resolved.sizes["time"],
            )
            sort_order = np.argsort(fp_time_resolved["time"].values, kind="stable")
            if not np.array_equal(sort_order, np.arange(sort_order.size)):
                restore_time_order = np.argsort(sort_order)
                fp_time_resolved = fp_time_resolved.isel(time=sort_order)
                fp_residual = fp_residual.isel(time=sort_order)
                low_frequency_flux = low_frequency_flux.isel(time=sort_order)

        fp_time_resolved, fp_residual, flux_da = _chunk_inputs(
            fp_time_resolved,
            fp_residual,
            flux_da,
            time_chunk=compute_time_chunk,
            lat_chunk=lat_chunk,
            lon_chunk=lon_chunk,
            source_chunk=source_chunk,
            minimum_time_chunk=(
                int(_hourly_lags(fp_time_resolved).max(initial=0)) if use_regular_kernel else 1
            ),
        )
        if use_regular_kernel:
            kernel_name = "numba_block_keep_space"
            lags = _hourly_lags(fp_time_resolved)
            max_lag = int(lags.max(initial=0))
            flux_halo = _flux_with_halo(flux_da, fp_time_resolved)
            fp_padded = _pad_footprint_left(fp_time_resolved, pad_hours=max_lag)

            resolved_data = da.blockwise(
                _resolved_keep_space_block,
                "tyxs",
                fp_padded.data,
                "tyxh",
                flux_halo.data,
                "tyxs",
                dtype=fp_padded.dtype,
                concatenate=True,
                adjust_chunks={
                    "t": fp_padded.data.chunks[0],
                    "y": fp_padded.data.chunks[1],
                    "x": fp_padded.data.chunks[2],
                    "s": flux_halo.data.chunks[3],
                },
                lag_indices=lags,
                meta=np.array((), dtype=fp_padded.dtype),
            )
            resolved = xr.DataArray(
                resolved_data,
                dims=("time", "lat", "lon", "source"),
                coords={
                    "time": fp_padded["time"],
                    "lat": fp_padded["lat"],
                    "lon": fp_padded["lon"],
                    "source": flux_da["source"],
                },
            ).isel(time=slice(max_lag, None))
            resolved = resolved.assign_coords(time=fp_ds["time"])
        else:
            kernel_name = "numba_indexed_keep_space"
            resolved = _resolved_keep_space_indexed(
                fp_time_resolved,
                flux_da,
                step_hours=aligned_flux_step_hours,
            )
    residual = fp_residual * low_frequency_flux
    result = cast(xr.DataArray, (resolved + residual).transpose(*REQUIRED_OUTPUT_DIMS))
    if restore_time_order is not None:
        result = result.isel(time=restore_time_order).assign_coords(time=fp_ds["time"])
    result = _attach_source_coordinates(result, flux_metadata)
    result.name = "fp_x_flux"
    result.attrs.update(
        {
            "units": "1",
            "operator": FP_X_FLUX_OPERATOR,
            "operator_version": FP_X_FLUX_OPERATOR_VERSION,
            "kernel": kernel_name,
            "fillna_zero": fillna_zero,
            "flux_time_alignment": FLUX_TIME_ALIGNMENT,
            "compute_time_chunk": compute_time_chunk,
            "description": "Footprint times source-resolved flux, retaining latitude and longitude.",
            "flux_units": flux_da.attrs.get("units", ""),
            "footprint_units": fp_time_resolved.attrs.get("units", ""),
        }
    )
    if time_selector is not None:
        selected_times = _normalise_time_selector(time_selector)
        missing = pd.DatetimeIndex(selected_times).difference(pd.DatetimeIndex(result["time"].values))
        if len(missing):
            raise ValueError(
                f"time_selector contains {len(missing)} timestamp(s) outside the result time grid."
            )
        result = result.sel(time=selected_times)
    output_chunks = {
        dim: size
        for dim, size in (
            (
                "time",
                (
                    compute_time_chunk
                    if time_chunk is None and kernel_name == "numba_indexed_keep_space"
                    else time_chunk
                ),
            ),
            ("lat", lat_chunk),
            ("lon", lon_chunk),
            ("source", source_chunk),
        )
        if size is not None
    }
    if output_chunks:
        result = result.chunk(output_chunks)
    return result


def warm_numba_fp_x_flux() -> str:
    """Compile the regular and indexed Numba kernels in the current process.

    This is optional. It is mainly useful with ``distributed.Client.run`` to
    remove first-call compilation cost from every Dask worker.
    """
    _require_numba()
    fp = np.zeros((1, 1, 1, 1), dtype=np.float32)
    flux = np.zeros((1, 1, 1, 1), dtype=np.float32)
    _resolved_keep_space_block(fp, flux, np.zeros(1, dtype=np.int64))
    _resolved_keep_space_indexed_block(fp, flux, np.zeros((1, 1), dtype=np.int64))
    return "numba fp_x_flux kernel ready"


def write_fp_x_flux_keep_space_zarr(  # noqa: PLR0913
    result: xr.DataArray,
    output_path: str | Path,
    *,
    output_chunks: Mapping[str, int] | None = None,
    provenance: Mapping[str, Any],
    overwrite: bool = False,
) -> Path:
    """Atomically persist a keep-space result as consolidated float32 ppm Zarr.

    A JSON manifest is written beside the store. ``provenance`` is supplied by
    the caller because OpenGHG cannot infer record versions, checksums, or an
    observation-selector identity from arbitrary xarray inputs.
    """
    if set(result.dims) != set(REQUIRED_OUTPUT_DIMS):
        raise ValueError(f"Result must contain exactly the dimensions {REQUIRED_OUTPUT_DIMS!r}.")
    if result.attrs.get("units") not in {"1", "mol mol-1", "mol/mol"}:
        raise ValueError("Native fp_x_flux values must be dimensionless mole fraction before writing.")
    required_provenance = {"footprint", "flux", "observation_time_selector"}
    missing = required_provenance - set(provenance)
    if missing:
        raise ValueError(f"provenance is missing required entries: {sorted(missing)}")

    path = Path(output_path).expanduser()
    if path.exists() and not overwrite:
        raise FileExistsError(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{uuid4().hex}")
    manifest_path = path.with_suffix(path.suffix + ".manifest.json")
    temporary_manifest = manifest_path.with_name(f".{manifest_path.name}.tmp-{uuid4().hex}")

    ppm = (result.astype(np.float32) * np.float32(1_000_000.0)).transpose(*REQUIRED_OUTPUT_DIMS)
    if output_chunks:
        ppm = ppm.chunk(dict(output_chunks))
    ppm.name = result.name or "fp_x_flux"
    ppm.attrs = dict(result.attrs)
    ppm.attrs["units"] = "ppm"
    ppm.attrs["native_units"] = "1"
    dataset = ppm.to_dataset()
    manifest = {
        "operator": FP_X_FLUX_OPERATOR,
        "operator_version": FP_X_FLUX_OPERATOR_VERSION,
        "kernel": result.attrs.get("kernel"),
        "kernel_options": {
            "fillna_zero": result.attrs.get("fillna_zero"),
            "flux_time_alignment": result.attrs.get("flux_time_alignment"),
            "compute_time_chunk": result.attrs.get("compute_time_chunk"),
        },
        "units": "ppm per unit source scaling",
        "source": [str(value) for value in result["source"].values],
        "grid": {
            "lat_size": result.sizes["lat"],
            "lon_size": result.sizes["lon"],
            "lat_min": float(result["lat"].min()),
            "lat_max": float(result["lat"].max()),
            "lon_min": float(result["lon"].min()),
            "lon_max": float(result["lon"].max()),
        },
        "time_coverage": {
            "start": str(result["time"].values[0]),
            "end": str(result["time"].values[-1]),
            "count": result.sizes["time"],
        },
        "provenance": dict(provenance),
    }
    try:
        dataset.to_zarr(temporary, mode="w", consolidated=True)
        temporary_manifest.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
        if overwrite and path.exists():
            backup = path.with_name(f".{path.name}.old-{uuid4().hex}")
            os.replace(path, backup)
            try:
                os.replace(temporary, path)
            except Exception:
                os.replace(backup, path)
                raise
            shutil.rmtree(backup)
        else:
            os.replace(temporary, path)
        os.replace(temporary_manifest, manifest_path)
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)
        temporary_manifest.unlink(missing_ok=True)
    return path
