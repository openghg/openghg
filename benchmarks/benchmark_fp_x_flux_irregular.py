"""Benchmark irregular-time keep-space fp x flux on the OCO-2 test data.

The stages are deliberately run in one process so Numba compilation is paid
once, outside the measured calculations. Each completed stage is written to
the output JSON immediately, leaving useful evidence if a later stage is too
slow or runs out of memory.
"""

from __future__ import annotations

import argparse
import gc
import hashlib
import json
import os
import resource
import socket
import time
from pathlib import Path
from typing import Any

import dask
import numpy as np
import xarray as xr
from dask.diagnostics import ResourceProfiler

from openghg.analyse import fp_x_flux_keep_space, fp_x_flux_keep_space_core, warm_numba_fp_x_flux

DEFAULT_ROOT = Path("/group/chem/acrg/object_stores/temp/OCO2_test/data")
DEFAULT_REPAIRED = Path(
    "/group/chem/acrg/object_stores/temp/OCO2_test/benchmarks/issue-1704-january-time32.zarr"
)
FOOTPRINT_RECORD = "02db7e30-80fa-4494-ae34-f9408a971582/zarr/v1"
FLUX_RECORD = "833dbac1-8bed-41f1-80af-57bd6464274c/zarr/v1"
START = np.datetime64("2023-01-01")
STOP = np.datetime64("2023-02-01")
REPAIR_VERSION = 1


def _json_value(value: Any) -> Any:
    """Convert NumPy and tuple values into JSON-compatible values."""
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, tuple):
        return [_json_value(item) for item in value]
    if isinstance(value, dict):
        return {key: _json_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_value(item) for item in value]
    return value


def _write_report(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(_json_value(report), indent=2, sort_keys=True) + "\n")
    temporary.replace(path)


def _time_checksum(times: np.ndarray) -> str:
    values = np.ascontiguousarray(times.astype("datetime64[ns]").astype(np.int64))
    return hashlib.sha256(values.view(np.uint8)).hexdigest()


def _physical_size(path: Path) -> int:
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def _time_layout(times: np.ndarray) -> dict[str, Any]:
    time_ns = times.astype("datetime64[ns]").astype(np.int64)
    sorted_ns = np.sort(time_ns, kind="stable")
    gaps_minutes = np.diff(sorted_ns) / (60 * 1_000_000_000)
    return {
        "monotonic_non_decreasing": bool(np.all(np.diff(time_ns) >= 0)),
        "descending_steps": int(np.count_nonzero(np.diff(time_ns) < 0)),
        "duplicate_times": int(time_ns.size - np.unique(time_ns).size),
        "sorted_gap_minutes": {
            "minimum": float(gaps_minutes.min(initial=np.inf)),
            "median": float(np.median(gaps_minutes)) if gaps_minutes.size else None,
            "maximum": float(gaps_minutes.max(initial=-np.inf)),
        },
    }


def _indexed_slab_layout(times: np.ndarray, h_back: np.ndarray, time_chunk: int) -> dict[str, Any]:
    """Estimate hourly flux slabs loaded by the indexed kernel's time blocks."""
    sorted_times = np.sort(times.astype("datetime64[ns]"), kind="stable")
    lag_hours = np.rint(h_back).astype(np.int64)
    slab_rows: list[int] = []
    target_rows = 0
    for start in range(0, sorted_times.size, time_chunk):
        block_times = sorted_times[start : start + time_chunk]
        targets = block_times[:, None] - lag_hours[None, :].astype("timedelta64[h]")
        hourly_rows = targets.astype("datetime64[h]").astype(np.int64)
        slab_rows.append(int(hourly_rows.max() - hourly_rows.min() + 1))
        target_rows += int(hourly_rows.size)
    total_slab_rows = sum(slab_rows)
    return {
        "blocks": len(slab_rows),
        "rows_per_block_minimum": min(slab_rows, default=0),
        "rows_per_block_mean": float(np.mean(slab_rows)) if slab_rows else 0.0,
        "rows_per_block_maximum": max(slab_rows, default=0),
        "total_hourly_slab_rows": total_slab_rows,
        "total_lag_targets": target_rows,
        "slab_rows_per_release": total_slab_rows / max(1, sorted_times.size),
        "slab_to_target_ratio": total_slab_rows / max(1, target_rows),
    }


def _chunks(array: xr.DataArray) -> dict[str, list[int]]:
    return {dim: [int(value) for value in chunks] for dim, chunks in array.chunksizes.items()}


def _chunk_summary(array: xr.DataArray) -> dict[str, dict[str, int]]:
    return {
        dim: {
            "count": len(chunks),
            "minimum": int(min(chunks)),
            "maximum": int(max(chunks)),
            "first": int(chunks[0]),
            "last": int(chunks[-1]),
        }
        for dim, chunks in array.chunksizes.items()
    }


def _prepare_core_inputs(
    footprint: xr.Dataset,
    flux: xr.DataArray,
    *,
    time_chunk: int,
) -> tuple[xr.Dataset, xr.DataArray]:
    """Establish the public core contract without using wrapper internals."""
    order = np.argsort(footprint["time"].values, kind="stable")
    prepared_footprint = (
        footprint[["fp_time_resolved", "fp_residual"]]
        .isel(time=order)
        .astype(np.float32)
        .fillna(0.0)
        .chunk({"time": time_chunk, "lat": -1, "lon": -1, "H_back": -1})
    )
    prepared_flux = _prepare_flux(flux, apply_value_policy=True)
    return prepared_footprint, prepared_flux


def _prepare_flux(flux: xr.DataArray, *, apply_value_policy: bool) -> xr.DataArray:
    source = str(flux.attrs.get("source") or flux.name or "source")
    prepared = flux.astype(np.float32).fillna(0.0) if apply_value_policy else flux
    return prepared.expand_dims(source=[source]).chunk({"lat": -1, "lon": -1, "source": 1})


def _core_readiness(footprint: xr.Dataset, flux: xr.DataArray) -> dict[str, Any]:
    resolved = footprint["fp_time_resolved"]
    spatial_coordinates_match = all(resolved[dim].equals(flux[dim]) for dim in ("lat", "lon"))
    spatial_coordinate_metadata_match = all(resolved[dim].identical(flux[dim]) for dim in ("lat", "lon"))
    return {
        "ready_without_wrapper": bool(
            _time_layout(footprint["time"].values)["monotonic_non_decreasing"]
            and resolved.chunksizes["H_back"] == (resolved.sizes["H_back"],)
            and spatial_coordinates_match
            and "source" in flux.dims
        ),
        "has_source_dimension": "source" in flux.dims,
        "single_h_back_chunk": resolved.chunksizes["H_back"] == (resolved.sizes["H_back"],),
        "spatial_coordinates_match": spatial_coordinates_match,
        "spatial_coordinate_metadata_match": spatial_coordinate_metadata_match,
        "footprint_chunks": _chunk_summary(resolved),
        "flux_chunks": _chunk_summary(flux),
    }


def _repair_footprint(
    footprint: xr.Dataset,
    output_path: Path,
    *,
    time_chunk: int,
    workers: int,
) -> tuple[xr.Dataset, dict[str, Any]]:
    """Create or validate a sorted, filled, compute-chunked January cache."""
    times = footprint["time"].values
    positions = np.flatnonzero((times >= START) & (times < STOP))
    positions = positions[np.argsort(times[positions], kind="stable")]
    expected_times = times[positions]
    expected_checksum = _time_checksum(expected_times)

    if output_path.exists():
        cached = xr.open_zarr(output_path, consolidated=True)
        valid = bool(
            cached.attrs.get("repair_version") == REPAIR_VERSION
            and cached.attrs.get("source_time_checksum") == expected_checksum
            and cached.sizes.get("time") == positions.size
            and _time_layout(cached["time"].values)["monotonic_non_decreasing"]
            and cached["fp_time_resolved"].chunksizes["H_back"] == (cached.sizes["H_back"],)
            and max(cached["fp_time_resolved"].chunksizes["time"]) <= time_chunk
        )
        if not valid:
            raise ValueError(
                f"Existing repaired cache does not satisfy this benchmark contract: {output_path}"
            )
        return cached, {
            "path": str(output_path),
            "reused": True,
            "write_seconds": 0.0,
            "physical_size_bytes": _physical_size(output_path),
            "release_count": int(cached.sizes["time"]),
            "chunks": _chunks(cached["fp_time_resolved"]),
            "time_checksum": expected_checksum,
        }

    repaired = (
        footprint[["fp_time_resolved", "fp_residual"]]
        .isel(time=positions)
        .astype(np.float32)
        .fillna(0.0)
        .chunk({"time": time_chunk, "lat": -1, "lon": -1, "H_back": -1})
    )
    repaired.attrs.update(
        {
            "repair_version": REPAIR_VERSION,
            "repair": "stable time sort, float32, NaN to zero, compute-aligned chunks",
            "source_zarr": str(footprint.encoding.get("source", "")),
            "source_time_checksum": expected_checksum,
        }
    )
    encoding = {
        "fp_time_resolved": {
            "chunks": (time_chunk, repaired.sizes["lat"], repaired.sizes["lon"], repaired.sizes["H_back"]),
            "compressor": footprint["fp_time_resolved"].encoding.get("compressor"),
        },
        "fp_residual": {
            "chunks": (time_chunk, repaired.sizes["lat"], repaired.sizes["lon"]),
            "compressor": footprint["fp_residual"].encoding.get("compressor"),
        },
    }
    for variable in repaired.variables:
        repaired[variable].encoding = {}
    temporary = output_path.with_name(
        f".{output_path.name}.tmp-{os.environ.get('SLURM_JOB_ID', os.getpid())}"
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    write_started = time.perf_counter()
    with dask.config.set(scheduler="threads", num_workers=workers):
        repaired.to_zarr(temporary, mode="w", consolidated=True, encoding=encoding)
    temporary.replace(output_path)
    write_seconds = time.perf_counter() - write_started
    cached = xr.open_zarr(output_path, consolidated=True)
    return cached, {
        "path": str(output_path),
        "reused": False,
        "write_seconds": write_seconds,
        "physical_size_bytes": _physical_size(output_path),
        "release_count": int(cached.sizes["time"]),
        "chunks": _chunks(cached["fp_time_resolved"]),
        "time_checksum": expected_checksum,
    }


def _graph_tasks(array: xr.DataArray) -> int:
    graph = array.data.__dask_graph__()
    return len(graph) if graph is not None else 0


def _run_eager_block(
    footprint: xr.Dataset,
    flux: xr.DataArray,
    *,
    time_chunk: int,
    workers: int,
) -> dict[str, Any]:
    """Measure one warm kernel block after all required inputs are in RAM."""
    times = footprint["time"].values
    positions = np.flatnonzero((times >= START) & (times < STOP))
    positions = positions[np.argsort(times[positions], kind="stable")][:time_chunk]
    max_lag = int(np.rint(footprint["H_back"].values).max(initial=0))
    flux_start = START - np.timedelta64(max_lag, "h")
    flux_stop = STOP - np.timedelta64(1, "ns")

    load_started = time.perf_counter()
    with dask.config.set(scheduler="threads", num_workers=workers):
        eager_footprint = (
            footprint[["fp_time_resolved", "fp_residual"]].isel(time=positions).astype(np.float32).load()
        )
        eager_flux = flux.sel(time=slice(flux_start, flux_stop)).astype(np.float32).load()
    load_seconds = time.perf_counter() - load_started
    footprint_missing_before_policy = sum(
        int(np.count_nonzero(np.isnan(eager_footprint[name].values)))
        for name in ("fp_time_resolved", "fp_residual")
    )
    flux_missing_before_policy = int(np.count_nonzero(np.isnan(eager_flux.values)))
    for name in ("fp_time_resolved", "fp_residual"):
        eager_footprint[name].data = np.nan_to_num(eager_footprint[name].values, copy=False)
    eager_flux.data = np.nan_to_num(eager_flux.values, copy=False)

    eager_footprint = eager_footprint.chunk({"time": time_chunk, "lat": -1, "lon": -1, "H_back": -1})
    prepared_flux = _prepare_flux(eager_flux, apply_value_policy=False)
    result = fp_x_flux_keep_space_core(eager_footprint, prepared_flux)
    compute_started = time.perf_counter()
    with dask.config.set(scheduler="threads", num_workers=workers):
        with ResourceProfiler(dt=0.05) as profile:
            checksum = float(result.sum().compute())
    compute_seconds = time.perf_counter() - compute_started
    memory_samples = [float(sample.mem) for sample in profile.results]
    metrics = {
        "release_count": int(eager_footprint.sizes["time"]),
        "input_load_seconds": load_seconds,
        "compute_seconds": compute_seconds,
        "checksum": checksum,
        "kernel": result.attrs["kernel"],
        "graph_tasks": _graph_tasks(result),
        "footprint_missing_values_before_policy": footprint_missing_before_policy,
        "flux_missing_values_before_policy": flux_missing_before_policy,
        "resource_memory_mb": {
            "minimum": min(memory_samples, default=None),
            "maximum": max(memory_samples, default=None),
            "increase": (max(memory_samples) - min(memory_samples) if memory_samples else None),
        },
        "process_max_rss_mb": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024,
    }
    del result, prepared_flux, eager_flux, eager_footprint
    gc.collect()
    return metrics


def _run_stage(
    footprint: xr.Dataset,
    flux: xr.DataArray,
    *,
    days: int,
    time_chunk: int,
    workers: int,
    compute_path: str,
) -> dict[str, Any]:
    end = START + np.timedelta64(days, "D")
    times = footprint["time"].values
    positions = np.flatnonzero((times >= START) & (times < end))
    if positions.size and np.all(np.diff(positions) == 1):
        sample = footprint.isel(time=slice(int(positions[0]), int(positions[-1]) + 1))
    else:
        sample = footprint.isel(time=positions)

    prepare_started = time.perf_counter()
    if compute_path == "core":
        prepared_footprint = sample[["fp_time_resolved", "fp_residual"]]
        prepared_flux = _prepare_flux(flux, apply_value_policy=True)
    else:
        prepared_footprint, prepared_flux = _prepare_core_inputs(sample, flux, time_chunk=time_chunk)
    prepare_seconds = time.perf_counter() - prepare_started

    core_graph_started = time.perf_counter()
    core_result = fp_x_flux_keep_space_core(prepared_footprint, prepared_flux)
    core_graph_seconds = time.perf_counter() - core_graph_started

    wrapper_graph_started = time.perf_counter()
    wrapper_result = fp_x_flux_keep_space(sample, flux, time_chunk=time_chunk)
    wrapper_graph_seconds = time.perf_counter() - wrapper_graph_started
    result = core_result if compute_path == "core" else wrapper_result

    stage: dict[str, Any] = {
        "days": days,
        "release_count": int(sample.sizes["time"]),
        "time_layout": _time_layout(sample["time"].values),
        "indexed_flux_layout": _indexed_slab_layout(
            sample["time"].values,
            sample["H_back"].values,
            time_chunk,
        ),
        "preparation_seconds": prepare_seconds,
        "core_graph_seconds": core_graph_seconds,
        "core_graph_tasks": _graph_tasks(core_result),
        "core_output_chunks": _chunks(core_result),
        "wrapper_graph_seconds": wrapper_graph_seconds,
        "wrapper_graph_tasks": _graph_tasks(wrapper_result),
        "wrapper_output_chunks": _chunks(wrapper_result),
        "wrapper_restored_input_order": bool(
            np.array_equal(wrapper_result["time"].values, sample["time"].values)
        ),
        "computed_path": compute_path,
        "kernel": result.attrs["kernel"],
    }

    compute_started = time.perf_counter()
    with dask.config.set(scheduler="threads", num_workers=workers):
        with ResourceProfiler(dt=0.2) as profile:
            checksum = float(result.sum().compute())
    compute_seconds = time.perf_counter() - compute_started
    memory_samples = [float(sample.mem) for sample in profile.results]
    stage.update(
        {
            "checksum": checksum,
            "compute_seconds": compute_seconds,
            "releases_per_second": sample.sizes["time"] / compute_seconds,
            "resource_memory_mb": {
                "minimum": min(memory_samples, default=None),
                "maximum": max(memory_samples, default=None),
                "increase": (max(memory_samples) - min(memory_samples) if memory_samples else None),
            },
            "process_max_rss_mb": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024,
        }
    )
    return stage


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--output-json", type=Path, required=True)
    parser.add_argument("--repaired-footprint", type=Path, default=DEFAULT_REPAIRED)
    parser.add_argument(
        "--benchmark-mode",
        choices=("both", "original", "repaired"),
        default="both",
        help="Run both storage layouts or isolate one layout in a fresh process.",
    )
    parser.add_argument("--stages", type=int, nargs="+", default=[1, 3, 7, 14, 31])
    parser.add_argument("--time-chunk", type=int, default=32)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument(
        "--max-stage-seconds",
        type=float,
        default=1200.0,
        help="Do not start the next stage if the last compute exceeds this duration.",
    )
    parser.add_argument(
        "--max-memory-mb",
        type=float,
        default=14000.0,
        help="Do not start the next stage if ResourceProfiler exceeds this value.",
    )
    return parser.parse_args()


def _run_stage_series(
    report: dict[str, Any],
    output_path: Path,
    *,
    report_key: str,
    footprint: xr.Dataset,
    flux: xr.DataArray,
    stages: list[int],
    time_chunk: int,
    workers: int,
    compute_path: str,
    max_stage_seconds: float,
    max_memory_mb: float,
) -> str:
    for days in stages:
        print(f"Starting {report_key} {days}-day stage", flush=True)
        stage = _run_stage(
            footprint,
            flux,
            days=days,
            time_chunk=time_chunk,
            workers=workers,
            compute_path=compute_path,
        )
        if report_key == "repaired_stages":
            original = next(
                (item for item in report["original_stages"] if item["days"] == days),
                None,
            )
            if original is not None:
                stage["original_checksum"] = original["checksum"]
                stage["checksum_difference"] = stage["checksum"] - original["checksum"]
                stage["checksum_matches_original"] = bool(
                    np.isclose(stage["checksum"], original["checksum"], rtol=1e-5, atol=1e-10)
                )
        report[report_key].append(stage)
        _write_report(output_path, report)
        print(json.dumps(_json_value(stage), sort_keys=True), flush=True)

        peak_memory = stage["process_max_rss_mb"]
        if stage["compute_seconds"] > max_stage_seconds:
            return "stopped_after_slow_stage"
        if peak_memory is not None and peak_memory > max_memory_mb:
            return "stopped_after_memory_threshold"
    return "complete"


def main() -> None:
    args = _parse_args()
    footprint_path = args.root / FOOTPRINT_RECORD
    flux_path = args.root / FLUX_RECORD
    footprint = xr.open_zarr(footprint_path, consolidated=True)
    flux = xr.open_zarr(flux_path, consolidated=True)["flux"]

    report: dict[str, Any] = {
        "status": "running",
        "host": socket.gethostname(),
        "slurm_job_id": os.environ.get("SLURM_JOB_ID"),
        "configuration": {
            "footprint": str(footprint_path),
            "flux": str(flux_path),
            "repaired_footprint": str(args.repaired_footprint),
            "benchmark_mode": args.benchmark_mode,
            "stages_days": args.stages,
            "time_chunk": args.time_chunk,
            "workers": args.workers,
            "max_stage_seconds": args.max_stage_seconds,
            "max_memory_mb": args.max_memory_mb,
        },
        "on_disk": {
            "release_count": int(footprint.sizes["time"]),
            "time_layout": _time_layout(footprint["time"].values),
            "core_readiness": _core_readiness(footprint, flux),
        },
        "original_stages": [],
        "repaired_stages": [],
    }

    try:
        warm_started = time.perf_counter()
        report["numba_warmup"] = {
            "message": warm_numba_fp_x_flux(),
            "seconds": time.perf_counter() - warm_started,
        }
        _write_report(args.output_json, report)

        statuses: list[str] = []
        if args.benchmark_mode in {"both", "original"}:
            print("Starting eager one-block measurement", flush=True)
            report["eager_block"] = _run_eager_block(
                footprint,
                flux,
                time_chunk=args.time_chunk,
                workers=args.workers,
            )
            _write_report(args.output_json, report)

            report["original_status"] = _run_stage_series(
                report,
                args.output_json,
                report_key="original_stages",
                footprint=footprint,
                flux=flux,
                stages=args.stages,
                time_chunk=args.time_chunk,
                workers=args.workers,
                compute_path="wrapper",
                max_stage_seconds=args.max_stage_seconds,
                max_memory_mb=args.max_memory_mb,
            )
            statuses.append(report["original_status"])

        if args.benchmark_mode in {"both", "repaired"}:
            print(f"Creating or validating repaired cache: {args.repaired_footprint}", flush=True)
            repaired, repair_metrics = _repair_footprint(
                footprint,
                args.repaired_footprint,
                time_chunk=args.time_chunk,
                workers=args.workers,
            )
            prepared_flux = _prepare_flux(flux, apply_value_policy=True)
            repair_metrics["core_readiness_with_prepared_flux"] = _core_readiness(
                repaired,
                prepared_flux,
            )
            report["repaired_cache"] = repair_metrics
            _write_report(args.output_json, report)

            report["repaired_status"] = _run_stage_series(
                report,
                args.output_json,
                report_key="repaired_stages",
                footprint=repaired,
                flux=flux,
                stages=args.stages,
                time_chunk=args.time_chunk,
                workers=args.workers,
                compute_path="core",
                max_stage_seconds=args.max_stage_seconds,
                max_memory_mb=args.max_memory_mb,
            )
            statuses.append(report["repaired_status"])

        report["status"] = "complete" if all(status == "complete" for status in statuses) else "partial"
    except Exception as error:
        report["status"] = "failed"
        report["error"] = {"type": type(error).__name__, "message": str(error)}
        _write_report(args.output_json, report)
        raise

    _write_report(args.output_json, report)
    print(f"Benchmark status: {report['status']}; results: {args.output_json}", flush=True)


if __name__ == "__main__":
    main()
