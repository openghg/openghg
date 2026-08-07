"""Benchmark the existing rolled OpenGHG fp x flux implementation on OCO-2."""

from __future__ import annotations

import argparse
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

from benchmarks.benchmark_fp_x_flux_irregular import (
    DEFAULT_ROOT,
    FLUX_RECORD,
    FOOTPRINT_RECORD,
    START,
    _chunk_summary,
    _graph_tasks,
    _json_value,
    _time_layout,
    _write_report,
)
from openghg.analyse._modelled_obs import fp_x_flux_time_resolved

DEFAULT_BASELINE = Path("/group/chem/acrg/object_stores/temp/OCO2_test/benchmarks/issue-1704-18300036.json")


def _run_stage(
    footprint: xr.Dataset,
    flux: xr.DataArray,
    *,
    days: int,
    workers: int,
    expected_checksum: float,
) -> dict[str, Any]:
    end = START + np.timedelta64(days, "D")
    times = footprint["time"].values
    positions = np.flatnonzero((times >= START) & (times < end))
    sample = footprint.isel(time=positions)
    order = np.argsort(sample["time"].values, kind="stable")
    sample = sample.isel(time=order)
    sample = sample.assign_coords({dim: sample[dim].assign_attrs(flux[dim].attrs) for dim in ("lat", "lon")})

    graph_started = time.perf_counter()
    result = fp_x_flux_time_resolved(sample, flux)
    graph_seconds = time.perf_counter() - graph_started

    compute_started = time.perf_counter()
    with dask.config.set(scheduler="threads", num_workers=workers):
        with ResourceProfiler(dt=0.2) as profile:
            checksum = float(result.sum().compute())
    compute_seconds = time.perf_counter() - compute_started
    memory_samples = [float(item.mem) for item in profile.results]
    return {
        "days": days,
        "release_count": int(sample.sizes["time"]),
        "time_layout_after_sort": _time_layout(sample["time"].values),
        "graph_seconds": graph_seconds,
        "graph_tasks": _graph_tasks(result),
        "output_chunks": _chunk_summary(result),
        "compute_seconds": compute_seconds,
        "releases_per_second": sample.sizes["time"] / compute_seconds,
        "checksum": checksum,
        "expected_checksum": expected_checksum,
        "checksum_difference": checksum - expected_checksum,
        "checksum_matches_numba": bool(np.isclose(checksum, expected_checksum, rtol=1e-5, atol=1e-10)),
        "resource_memory_mb": {
            "minimum": min(memory_samples, default=None),
            "maximum": max(memory_samples, default=None),
            "increase": (max(memory_samples) - min(memory_samples) if memory_samples else None),
        },
        "process_max_rss_mb": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024,
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--baseline-json", type=Path, default=DEFAULT_BASELINE)
    parser.add_argument("--output-json", type=Path, required=True)
    parser.add_argument("--stages", type=int, nargs="+", default=[1, 3, 7, 14, 31])
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--max-stage-seconds", type=float, default=1200.0)
    parser.add_argument("--max-memory-mb", type=float, default=14000.0)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    footprint_path = args.root / FOOTPRINT_RECORD
    flux_path = args.root / FLUX_RECORD
    footprint = xr.open_zarr(footprint_path, consolidated=True)
    flux = xr.open_zarr(flux_path, consolidated=True)["flux"]
    baseline = json.loads(args.baseline_json.read_text())
    expected = {int(item["days"]): float(item["checksum"]) for item in baseline["original_stages"]}

    report: dict[str, Any] = {
        "status": "running",
        "host": socket.gethostname(),
        "slurm_job_id": os.environ.get("SLURM_JOB_ID"),
        "method": "openghg.analyse._modelled_obs.fp_x_flux_time_resolved",
        "configuration": {
            "footprint": str(footprint_path),
            "flux": str(flux_path),
            "baseline_json": str(args.baseline_json),
            "stages_days": args.stages,
            "workers": args.workers,
            "max_stage_seconds": args.max_stage_seconds,
            "max_memory_mb": args.max_memory_mb,
            "time_preparation": "stable sort before legacy operator",
            "coordinate_preparation": (
                "copy flux lat/lon attributes onto equal footprint indexes to avoid PintIndex conflict"
            ),
        },
        "stages": [],
    }
    _write_report(args.output_json, report)

    try:
        for days in args.stages:
            print(f"Starting legacy {days}-day stage", flush=True)
            stage = _run_stage(
                footprint,
                flux,
                days=days,
                workers=args.workers,
                expected_checksum=expected[days],
            )
            report["stages"].append(stage)
            _write_report(args.output_json, report)
            print(json.dumps(_json_value(stage), sort_keys=True), flush=True)

            if not stage["checksum_matches_numba"]:
                report["status"] = "stopped_after_checksum_mismatch"
                break
            if stage["compute_seconds"] > args.max_stage_seconds:
                report["status"] = "stopped_after_slow_stage"
                break
            if stage["process_max_rss_mb"] > args.max_memory_mb:
                report["status"] = "stopped_after_memory_threshold"
                break
        else:
            report["status"] = "complete"
    except Exception as error:
        report["status"] = "failed"
        report["error"] = {"type": type(error).__name__, "message": str(error)}
        _write_report(args.output_json, report)
        raise

    _write_report(args.output_json, report)
    print(f"Benchmark status: {report['status']}; results: {args.output_json}", flush=True)


if __name__ == "__main__":
    main()
