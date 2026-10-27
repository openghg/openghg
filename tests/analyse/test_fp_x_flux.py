from __future__ import annotations

import json

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from openghg.analyse import (
    align_flux_to_time_targets,
    fp_x_flux_time_resolved_numba,
    fp_x_flux_time_resolved_numba_core,
    warm_numba_fp_x_flux,
    write_fp_x_flux_zarr,
)
from openghg.analyse._modelled_obs import fp_x_flux_time_resolved


@pytest.fixture(scope="module", autouse=True)
def data_read() -> None:
    """Override the analyse integration-data fixture for these unit tests."""


def _inputs() -> tuple[xr.Dataset, xr.DataArray]:
    """Build compact resolved-footprint and source-resolved flux fixtures."""
    fp_time = pd.date_range("2021-01-01 03:00", periods=5, freq="h")
    flux_time = pd.date_range("2021-01-01", periods=40, freq="h")
    lat = np.array([51.0, 52.0], dtype=np.float32)
    lon = np.array([-2.0, -1.0], dtype=np.float32)
    h_back = np.array([0, 1, 2], dtype=np.int64)

    resolved = xr.DataArray(
        np.arange(5 * 2 * 2 * 3, dtype=np.float32).reshape(5, 2, 2, 3) / 100,
        dims=("time", "lat", "lon", "H_back"),
        coords={"time": fp_time, "lat": lat, "lon": lon, "H_back": h_back},
    )
    resolved["H_back"].attrs["units"] = "hours"
    residual = xr.DataArray(
        np.ones((5, 2, 2), dtype=np.float32),
        dims=("time", "lat", "lon"),
        coords={"time": fp_time, "lat": lat, "lon": lon},
    )
    footprint = xr.Dataset({"fp_time_resolved": resolved, "fp_residual": residual})

    flux = xr.DataArray(
        np.arange(40 * 2 * 2 * 2, dtype=np.float32).reshape(40, 2, 2, 2) / 10,
        dims=("time", "lat", "lon", "source"),
        coords={
            "time": flux_time,
            "lat": lat,
            "lon": lon,
            "source": ["bio", "ff"],
            "species": ("source", ["co2", "co2"]),
            "scenario": ("source", ["BASE", "BASE"]),
            "sector": ("source", ["GPP", "FF"]),
        },
    )
    return footprint, flux


def _reference(footprint: xr.Dataset, flux: xr.DataArray) -> xr.DataArray:
    """Calculate expected hourly spatially resolved values directly with xarray."""
    resolved = None
    for lag in footprint["H_back"].values:
        fp_lag = footprint["fp_time_resolved"].sel(H_back=lag, drop=True)
        shifted = fp_lag["time"].values - np.timedelta64(int(lag), "h")
        flux_lag = flux.sel(time=shifted).assign_coords(time=fp_lag["time"])
        term = fp_lag * flux_lag
        resolved = term if resolved is None else resolved + term
    low_frequency = flux.resample(time="1MS").mean().reindex(time=footprint["time"], method="ffill")
    return (resolved + footprint["fp_residual"] * low_frequency).transpose("source", "lat", "lon", "time")


def _existing_time_resolved_reference(footprint: xr.Dataset, flux: xr.DataArray) -> xr.DataArray:
    """Evaluate the existing operator after adapting source dimensions."""
    if "source" not in flux.dims:
        return fp_x_flux_time_resolved(footprint, flux).expand_dims(source=["source"])
    results = [
        fp_x_flux_time_resolved(footprint, flux.sel(source=source, drop=True))
        for source in flux["source"].values
    ]
    return xr.concat(results, dim=flux["source"]).transpose("source", "lat", "lon", "time")


def _assert_value_parity(actual: xr.DataArray, expected: xr.DataArray) -> None:
    """Compare operator values while allowing the enhanced metadata coordinates."""
    xr.testing.assert_allclose(actual.reset_coords(drop=True), expected.reset_coords(drop=True))


def _interval_start_reference(footprint: xr.Dataset, flux: xr.DataArray) -> xr.DataArray:
    """Calculate expected irregular-time values by interval membership."""
    resolved = None
    for lag in footprint["H_back"].values:
        fp_lag = footprint["fp_time_resolved"].sel(H_back=lag, drop=True)
        targets = fp_lag["time"].values - np.timedelta64(int(lag), "h")
        starts = flux.time.values
        cadence = starts[1] - starts[0] if starts.size > 1 else np.timedelta64(1, "h")
        indexes = []
        for target in targets:
            matches = [i for i, start in enumerate(starts) if start <= target < start + cadence]
            assert len(matches) == 1
            indexes.append(matches[0])
        flux_lag = flux.isel(time=indexes).assign_coords(time=fp_lag["time"])
        term = fp_lag * flux_lag
        resolved = term if resolved is None else resolved + term
    flux_months = pd.DatetimeIndex(flux.time.values).to_period("M")
    monthly_values = [
        flux.isel(time=np.flatnonzero(flux_months == month)).mean("time")
        for month in pd.DatetimeIndex(footprint.time.values).to_period("M")
    ]
    low_frequency = xr.concat(monthly_values, dim=footprint.time)
    return (resolved + footprint["fp_residual"] * low_frequency).transpose("source", "lat", "lon", "time")


def test_fp_x_flux_time_resolved_numba_matches_reference_and_preserves_source_metadata() -> None:
    """Match direct values and preserve source metadata coordinates."""
    footprint, flux = _inputs()
    result = fp_x_flux_time_resolved_numba(
        footprint, flux, time_chunk=2, lat_chunk=1, lon_chunk=1, source_chunk=1
    )

    assert hasattr(result.data, "__dask_graph__")
    assert result.dims == ("source", "lat", "lon", "time")
    assert result.dtype == np.float32
    assert result.attrs["units"] == "1"
    assert result["species"].values.tolist() == ["co2", "co2"]
    assert result["scenario"].values.tolist() == ["BASE", "BASE"]
    assert result["sector"].values.tolist() == ["GPP", "FF"]
    xr.testing.assert_allclose(result.compute(), _reference(footprint, flux))


def test_fp_x_flux_time_resolved_numba_matches_existing_method() -> None:
    """Match the existing time-resolved operator on its supported domain."""
    footprint, flux = _inputs()

    result = fp_x_flux_time_resolved_numba(footprint, flux).compute()
    expected = _existing_time_resolved_reference(footprint, flux)

    _assert_value_parity(result, expected.astype(np.float32))


def test_time_selector_is_applied_to_finished_lazy_result() -> None:
    """Apply observation-time selection only after graph construction."""
    footprint, flux = _inputs()
    selected = footprint["time"].values[[1, 3]]

    result = fp_x_flux_time_resolved_numba(footprint, flux, time_selector=selected, time_chunk=2)

    assert result["time"].values.tolist() == selected.tolist()
    xr.testing.assert_allclose(result.compute(), _reference(footprint, flux).sel(time=selected))


def test_sources_can_be_selected_and_relabelled() -> None:
    """Select and relabel source coordinates without changing values."""
    footprint, flux = _inputs()

    result = fp_x_flux_time_resolved_numba(footprint, flux, sources=["ff"], source_labels=["fossil_fuel"])

    assert result["source"].values.tolist() == ["fossil_fuel"]
    assert result["sector"].values.tolist() == ["FF"]
    expected = _reference(footprint, flux.sel(source=["ff"])).assign_coords(source=["fossil_fuel"])
    xr.testing.assert_allclose(result.compute(), expected)


def test_single_source_flux_without_source_dimension_is_supported() -> None:
    """Promote flux without a source dimension to one labelled source."""
    footprint, flux = _inputs()
    single_flux = flux.sel(source="bio", drop=True)

    result = fp_x_flux_time_resolved_numba(footprint, single_flux, source_labels=["biosphere"])
    expected = fp_x_flux_time_resolved(footprint, single_flux).expand_dims(source=["biosphere"])

    assert result.dims == ("source", "lat", "lon", "time")
    xr.testing.assert_allclose(result.compute(), expected.transpose(*result.dims).astype(np.float32))


@pytest.mark.parametrize("step_hours", [2, 24])
def test_regular_coarse_flux_matches_existing_time_resolved_method(step_hours: int) -> None:
    """Match the legacy result for supported coarse flux cadences."""
    footprint, flux = _inputs()
    end = flux["time"].values[-1] + np.timedelta64(1, "h")
    coarse_time = pd.date_range(flux["time"].values[0], end, freq=f"{step_hours}h", inclusive="left")
    coarse_flux = flux.reindex(time=coarse_time, method="nearest")

    result = fp_x_flux_time_resolved_numba(footprint, coarse_flux).compute()
    expected = _existing_time_resolved_reference(footprint, coarse_flux)

    _assert_value_parity(result, expected.astype(np.float32))


def test_align_flux_to_time_targets_uses_left_labelled_interval_membership() -> None:
    """Map targets to containing left-labelled flux intervals."""
    times = pd.date_range("2021-01-01", periods=3, freq="h")
    flux = xr.DataArray(
        np.array([10.0, 20.0, 30.0]),
        dims="time",
        coords={"time": times},
    )
    targets = pd.to_datetime(
        [
            "2021-01-01 01:59:59",
            "2021-01-01 00:00:00",
            "2021-01-01 01:00:00",
            "2021-01-01 01:59:59",
            "2021-01-01 02:59:59",
        ]
    )

    result = align_flux_to_time_targets(flux, targets)

    assert result.values.tolist() == [20.0, 10.0, 20.0, 20.0, 30.0]
    assert result["time"].values.tolist() == targets.values.tolist()


@pytest.mark.parametrize(
    "targets",
    [["2020-12-31 23:59:59"], ["2021-01-01 03:00:00"]],
)
def test_align_flux_to_time_targets_rejects_times_outside_intervals(targets: list[str]) -> None:
    """Reject targets before coverage or beyond the final flux interval."""
    flux = xr.DataArray(
        np.ones(3),
        dims="time",
        coords={"time": pd.date_range("2021-01-01", periods=3, freq="h")},
    )

    with pytest.raises(ValueError, match="does not contain 1 target"):
        align_flux_to_time_targets(flux, targets)


def test_align_flux_to_time_targets_rejects_gapped_flux() -> None:
    """Reject an irregular flux grid that leaves unrepresented intervals."""
    flux = xr.DataArray(
        np.ones(3),
        dims="time",
        coords={"time": pd.to_datetime(["2021-01-01 00:00", "2021-01-01 01:00", "2021-01-01 03:00"])},
    )

    with pytest.raises(ValueError, match="must be regular"):
        align_flux_to_time_targets(flux, ["2021-01-01 02:00"])


def test_irregular_release_times_use_indexed_kernel_and_preserve_exact_times() -> None:
    """Use indexed lookup while preserving irregular order and duplicates."""
    footprint, flux = _inputs()
    irregular = pd.to_datetime(
        [
            "2021-01-01 06:37",
            "2021-01-01 03:23",
            "2021-01-01 09:58",
            "2021-01-01 06:37",
            "2021-01-01 12:01",
        ]
    )
    footprint = footprint.assign_coords(time=irregular)

    result = fp_x_flux_time_resolved_numba(
        footprint,
        flux,
        time_chunk=2,
        lat_chunk=1,
        lon_chunk=1,
        source_chunk=1,
    )

    assert result.attrs["kernel"] == "numba_indexed"
    assert result.attrs["flux_time_alignment"] == "interval_start"
    assert result["time"].values.tolist() == irregular.values.tolist()
    assert result.chunksizes["time"] == (2, 2, 1)
    assert hasattr(result.data, "__dask_graph__")
    assert result["sector"].values.tolist() == ["GPP", "FF"]
    xr.testing.assert_allclose(result.compute(), _interval_start_reference(footprint, flux))


def test_gapped_hour_aligned_release_times_use_indexed_kernel() -> None:
    """Use indexed lookup when otherwise hourly releases contain gaps."""
    footprint, flux = _inputs()
    footprint = footprint.assign_coords(
        time=pd.to_datetime(
            [
                "2021-01-01 03:00",
                "2021-01-01 04:00",
                "2021-01-01 08:00",
                "2021-01-01 11:00",
                "2021-01-01 12:00",
            ]
        )
    )

    result = fp_x_flux_time_resolved_numba(footprint, flux)

    assert result.attrs["kernel"] == "numba_indexed"
    xr.testing.assert_allclose(result.compute(), _interval_start_reference(footprint, flux))


def test_irregular_release_times_support_regular_coarse_flux() -> None:
    """Match the corrected legacy operator for irregular times and coarse flux."""
    footprint, flux = _inputs()
    footprint = footprint.assign_coords(
        time=pd.to_datetime(
            [
                "2021-01-01 02:37",
                "2021-01-01 05:23",
                "2021-01-01 07:58",
                "2021-01-01 09:01",
                "2021-01-01 11:42",
            ]
        )
    )
    coarse_flux = flux.isel(time=slice(None, None, 2))

    result = fp_x_flux_time_resolved_numba(footprint, coarse_flux).compute()
    expected = _existing_time_resolved_reference(footprint, coarse_flux)

    assert result.attrs["kernel"] == "numba_indexed"
    _assert_value_parity(result, expected.astype(np.float32))


def test_irregular_times_match_corrected_legacy_operator() -> None:
    """Match corrected legacy values for irregular footprint releases."""
    footprint, flux = _inputs()
    release_times = pd.to_datetime(
        [
            "2021-01-01 02:37",
            "2021-01-01 05:23",
            "2021-01-01 07:58",
            "2021-01-01 09:01",
            "2021-01-01 11:42",
        ]
    )
    footprint = footprint.assign_coords(time=release_times)

    result = fp_x_flux_time_resolved_numba(footprint, flux).compute()
    expected = _existing_time_resolved_reference(footprint, flux)

    _assert_value_parity(result, expected.astype(np.float32))


def test_regular_hourly_release_times_keep_existing_numba_kernel() -> None:
    """Retain the regular halo kernel for consecutive hourly releases."""
    footprint, flux = _inputs()

    result = fp_x_flux_time_resolved_numba(footprint, flux)

    assert result.attrs["kernel"] == "numba_block"


def test_irregular_release_times_default_to_bounded_time_chunks() -> None:
    """Default irregular computation to bounded release-time chunks."""
    footprint, flux = _inputs()
    footprint = footprint.isel(time=np.zeros(40, dtype=int)).assign_coords(
        time=pd.date_range("2021-01-01 03:01", periods=40, freq="53min")
    )

    result = fp_x_flux_time_resolved_numba(footprint, flux)

    assert result.attrs["compute_time_chunk"] == 32
    assert result.chunksizes["time"] == (32, 8)


def test_prepared_core_matches_wrapper_without_sorting_or_rechunking() -> None:
    """Match wrapper values when inputs already satisfy the core contract."""
    footprint, flux = _inputs()
    times = pd.to_datetime(
        [
            "2021-01-01 03:37",
            "2021-01-01 05:23",
            "2021-01-01 07:58",
            "2021-01-01 09:01",
            "2021-01-01 11:42",
        ]
    )
    footprint = footprint.assign_coords(time=times).chunk({"time": 2, "lat": 1, "lon": 1, "H_back": -1})
    flux = flux.chunk({"lat": 1, "lon": 1, "source": 1})

    core_result = fp_x_flux_time_resolved_numba_core(footprint, flux)
    wrapped_result = fp_x_flux_time_resolved_numba(
        footprint,
        flux,
        time_chunk=2,
        lat_chunk=1,
        lon_chunk=1,
        source_chunk=1,
    )

    assert core_result.attrs["kernel"] == "numba_indexed"
    assert core_result.chunksizes["time"] == footprint.chunksizes["time"]
    xr.testing.assert_allclose(core_result.compute(), wrapped_result.compute())


def test_prepared_core_rejects_non_monotonic_release_time() -> None:
    """Reject non-monotonic release times at the prepared core boundary."""
    footprint, flux = _inputs()
    footprint = footprint.assign_coords(time=footprint["time"].values[[0, 2, 1, 3, 4]]).chunk(
        {"time": 2, "lat": 1, "lon": 1, "H_back": -1}
    )
    flux = flux.chunk({"lat": 1, "lon": 1, "source": 1})

    with pytest.raises(ValueError, match="monotonic non-decreasing"):
        fp_x_flux_time_resolved_numba_core(footprint, flux)


def test_wrapper_sorts_hourly_releases_for_core_then_restores_input_order() -> None:
    """Sort for core computation and restore the caller's release order."""
    footprint, flux = _inputs()
    order = [0, 2, 1, 4, 3]
    footprint = footprint.isel(time=order)

    result = fp_x_flux_time_resolved_numba(footprint, flux, time_chunk=2)
    expected = _reference(footprint, flux)

    assert result.attrs["kernel"] == "numba_block"
    np.testing.assert_array_equal(result["time"], footprint["time"])
    xr.testing.assert_allclose(result.compute(), expected)


def test_irregular_release_time_selector_is_applied_after_complete_result() -> None:
    """Filter irregular releases only after computing the complete field."""
    footprint, flux = _inputs()
    footprint = footprint.assign_coords(
        time=pd.to_datetime(
            [
                "2021-01-01 03:37",
                "2021-01-01 05:23",
                "2021-01-01 07:58",
                "2021-01-01 09:01",
                "2021-01-01 11:42",
            ]
        )
    )
    selected = footprint["time"].values[[1, 4]]

    result = fp_x_flux_time_resolved_numba(footprint, flux, time_selector=selected)
    expected = _interval_start_reference(footprint, flux).sel(time=selected)

    assert result["time"].values.tolist() == selected.tolist()
    xr.testing.assert_allclose(result.compute(), expected)


def test_time_selector_rejects_duplicate_selector_values() -> None:
    """Reject ambiguous duplicate values in an observation-time selector."""
    footprint, flux = _inputs()
    selected = footprint["time"].values[[1, 1]]

    with pytest.raises(ValueError, match="time_selector must contain unique timestamps"):
        fp_x_flux_time_resolved_numba(footprint, flux, time_selector=selected)


def test_time_selector_rejects_non_unique_result_times() -> None:
    """Require positional alignment when footprint release times repeat."""
    footprint, flux = _inputs()
    duplicate_times = footprint["time"].values.copy()
    duplicate_times[3] = duplicate_times[1]
    footprint = footprint.assign_coords(time=duplicate_times)

    with pytest.raises(ValueError, match="result time coordinate contains duplicate timestamps"):
        fp_x_flux_time_resolved_numba(
            footprint,
            flux,
            time_selector=duplicate_times[[1]],
        )


def test_irregular_release_times_support_single_source_flux() -> None:
    """Support irregular releases with flux lacking a source dimension."""
    footprint, flux = _inputs()
    footprint = footprint.assign_coords(
        time=pd.to_datetime(
            [
                "2021-01-01 03:37",
                "2021-01-01 05:23",
                "2021-01-01 07:58",
                "2021-01-01 09:01",
                "2021-01-01 11:42",
            ]
        )
    )
    flux = flux.sel(source="bio", drop=True)

    result = fp_x_flux_time_resolved_numba(footprint, flux, source_labels=["biosphere"])
    expected = _interval_start_reference(
        footprint,
        flux.expand_dims(source=["biosphere"]),
    )

    xr.testing.assert_allclose(result.compute(), expected)


def test_month_boundary_residual_uses_the_containing_month() -> None:
    """Use the calendar month containing each release for residual flux."""
    times = pd.to_datetime(["2021-01-31 23:37", "2021-02-01 00:23"])
    resolved = xr.DataArray(
        np.zeros((2, 1, 1, 1), dtype=np.float32),
        dims=("time", "lat", "lon", "H_back"),
        coords={"time": times, "lat": [51.0], "lon": [-1.0], "H_back": [0]},
    )
    resolved["H_back"].attrs["units"] = "hours"
    residual = xr.DataArray(
        np.ones((2, 1, 1), dtype=np.float32),
        dims=("time", "lat", "lon"),
        coords={"time": times, "lat": [51.0], "lon": [-1.0]},
    )
    footprint = xr.Dataset({"fp_time_resolved": resolved, "fp_residual": residual})
    flux_times = pd.date_range("2021-01-31", "2021-02-02", freq="h", inclusive="left")
    flux_values = np.where(flux_times.month == 1, 1.0, 3.0).astype(np.float32)
    flux = xr.DataArray(
        flux_values.reshape(-1, 1, 1),
        dims=("time", "lat", "lon"),
        coords={"time": flux_times, "lat": [51.0], "lon": [-1.0]},
    )

    result = fp_x_flux_time_resolved_numba(footprint, flux).compute()

    np.testing.assert_allclose(result.values.reshape(-1), [1.0, 3.0])


def test_coarse_flux_interval_membership_crosses_month_boundary() -> None:
    """Map coarse flux intervals correctly across a calendar boundary."""
    release_times = pd.to_datetime(["2021-01-31 23:37", "2021-02-01 00:23"])
    resolved = xr.DataArray(
        np.ones((2, 1, 1, 2), dtype=np.float32),
        dims=("time", "lat", "lon", "H_back"),
        coords={"time": release_times, "lat": [51.0], "lon": [-1.0], "H_back": [0, 1]},
    )
    resolved["H_back"].attrs["units"] = "hours"
    residual = xr.zeros_like(resolved.isel(H_back=0, drop=True))
    footprint = xr.Dataset({"fp_time_resolved": resolved, "fp_residual": residual})
    flux_times = pd.date_range("2021-01-31 20:00", periods=6, freq="2h")
    flux = xr.DataArray(
        np.arange(6, dtype=np.float32).reshape(-1, 1, 1),
        dims=("time", "lat", "lon"),
        coords={"time": flux_times, "lat": [51.0], "lon": [-1.0]},
    )

    result = fp_x_flux_time_resolved_numba(footprint, flux).compute()

    np.testing.assert_allclose(result.values.reshape(-1), [2.0, 3.0])


def test_flux_time_labels_must_describe_interval_starts() -> None:
    """Reject flux explicitly labelled with non-start timestamps."""
    footprint, flux = _inputs()
    flux["time"].attrs["label"] = "right"

    with pytest.raises(ValueError, match="must label the start"):
        fp_x_flux_time_resolved_numba(footprint, flux)


def test_time_selector_rejects_times_outside_footprint_grid() -> None:
    """Reject selected timestamps absent from the footprint releases."""
    footprint, flux = _inputs()

    with pytest.raises(ValueError, match="outside the result time grid"):
        fp_x_flux_time_resolved_numba(footprint, flux, time_selector=["2022-01-01"])


def test_flux_must_include_complete_lag_halo() -> None:
    """Reject flux that omits any release-minus-lag target interval."""
    footprint, flux = _inputs()
    flux = flux.sel(time=slice(footprint["time"].values[0], None))

    with pytest.raises(ValueError, match="complete footprint lag halo"):
        fp_x_flux_time_resolved_numba(footprint, flux)


@pytest.mark.parametrize("time_indices", [[3, 27], [0, 24]])
def test_low_frequency_flux_must_cover_complete_footprint_period(time_indices: list[int]) -> None:
    """Validate lag and release coverage before using low-frequency flux.

    Args:
        time_indices: Flux indexes producing missing lag or release coverage.
    """
    footprint, flux = _inputs()
    coarse_flux = flux.isel(time=time_indices)
    if time_indices[0] == 0:
        footprint = footprint.assign_coords(time=footprint["time"] + np.timedelta64(60, "D"))

    with pytest.raises(ValueError, match="complete footprint lag halo"):
        fp_x_flux_time_resolved_numba(footprint, coarse_flux)


def test_write_fp_x_flux_zarr_writes_ppm_and_manifest(tmp_path) -> None:
    """Persist float32 ppm values and the required provenance manifest."""
    footprint, flux = _inputs()
    result = fp_x_flux_time_resolved_numba(
        footprint,
        flux,
        time_selector=footprint["time"].values[[0, 2]],
    ).isel(time=[1, 0])
    path = tmp_path / "cache.zarr"

    returned = write_fp_x_flux_zarr(
        result,
        path,
        output_chunks={"source": 1, "time": 1},
        provenance={
            "footprint": {"version": "test-fp"},
            "flux": {"checksum": "test-flux"},
            "observation_time_selector": {"checksum": "test-times"},
        },
    )

    stored = xr.open_zarr(path, consolidated=True)["fp_x_flux"]
    assert returned == path
    assert stored.attrs["units"] == "ppm"
    xr.testing.assert_allclose(stored, result * np.float32(1_000_000))
    manifest = json.loads((tmp_path / "cache.zarr.manifest.json").read_text())
    assert manifest["operator_version"] == 2
    assert manifest["kernel_options"]["flux_time_alignment"] == "interval_start"
    assert manifest["source"] == ["bio", "ff"]
    assert manifest["provenance"]["flux"]["checksum"] == "test-flux"
    assert manifest["time_coverage"] == {
        "start": str(result["time"].values.min()),
        "end": str(result["time"].values.max()),
        "count": 2,
    }


def test_warm_numba_fp_x_flux() -> None:
    """Compile both Numba kernels and return the readiness message."""
    assert warm_numba_fp_x_flux() == "numba fp_x_flux kernel ready"


@pytest.mark.parametrize("cast_float32", [True, False])
def test_physical_unit_scales_include_independent_residual(cast_float32: bool) -> None:
    """Normalize declared units on each field before selecting precision."""
    footprint, flux = _inputs()
    footprint = footprint.astype(np.float64)
    flux = flux.astype(np.float64)
    expected = fp_x_flux_time_resolved_numba(footprint, flux, cast_float32=cast_float32).compute()
    scaled = footprint.copy(deep=True)
    scaled["fp_time_resolved"] = scaled["fp_time_resolved"] * 1e-6
    scaled["fp_time_resolved"].attrs["units"] = "m2 s umol-1"
    scaled["fp_residual"] = scaled["fp_residual"] * 1e-6
    scaled["fp_residual"].attrs["units"] = "km2 s mol-1"
    scaled_flux = flux * 1e6
    scaled_flux.attrs["units"] = "umol m-2 s-1"

    actual = fp_x_flux_time_resolved_numba(scaled, scaled_flux, cast_float32=cast_float32)

    assert actual.attrs["units"] == "1"
    xr.testing.assert_allclose(actual.compute(), expected)


@pytest.mark.parametrize("field", ["fp_time_resolved", "fp_residual", "flux"])
@pytest.mark.parametrize("units", ["1", "kg"])
def test_incompatible_physical_units_are_rejected(field: str, units: str) -> None:
    """An explicit dimensionless unit is not an absent physical unit."""
    footprint, flux = _inputs()
    (flux if field == "flux" else footprint[field]).attrs["units"] = units
    with pytest.raises(ValueError, match="units.*compatible"):
        fp_x_flux_time_resolved_numba(footprint, flux)


@pytest.mark.parametrize("field", ["fp_time_resolved", "fp_residual", "flux"])
def test_core_rejects_scaled_units(field: str) -> None:
    """The prepared core validates scale without converting borrowed data."""
    footprint, flux = _inputs()
    footprint = footprint.chunk({"time": 2, "H_back": -1})
    flux = flux.chunk({"time": 8})
    (flux if field == "flux" else footprint[field]).attrs["units"] = (
        "umol m-2 s-1" if field == "flux" else "m2 s umol-1"
    )
    with pytest.raises(ValueError, match="canonical scale"):
        fp_x_flux_time_resolved_numba_core(footprint, flux)


@pytest.mark.parametrize("kernel", ["numba_block", "numba_indexed", "low_frequency"])
@pytest.mark.parametrize("double_field", ["fp_residual", "flux"])
def test_mixed_precision_preserves_nonzero_residual(kernel: str, double_field: str) -> None:
    """Keep small float64 contributions through all three execution paths."""
    footprint, flux = _inputs()
    footprint = xr.ones_like(footprint)
    flux = xr.ones_like(flux)
    if double_field == "flux":
        flux = flux.astype(np.float64) + 2.0**-30
    else:
        footprint["fp_residual"] = footprint["fp_residual"].astype(np.float64) + 2.0**-30
    if kernel == "numba_indexed":
        footprint = footprint.assign_coords(time=footprint.time + np.timedelta64(17, "m"))
    elif kernel == "low_frequency":
        flux = flux.isel(time=slice(None, None, 4))
    result = fp_x_flux_time_resolved_numba(footprint, flux, cast_float32=False, time_chunk=2)
    expected = (4 + 2.0**-30) if double_field == "fp_residual" else 4 * (1 + 2.0**-30)

    assert result.attrs["kernel"] == kernel
    assert result.dtype == np.float64
    computed = result.compute()
    assert computed.dtype == np.float64
    np.testing.assert_allclose(computed.values, expected, rtol=0, atol=1e-14)
    assert result.chunksizes["time"] == (2, 2, 1)


def test_core_rejects_mixed_dtypes_and_wrapper_promotes_integer_inputs() -> None:
    """Prepared values have one honest kernel dtype; wrapper prepares integers."""
    footprint, flux = _inputs()
    footprint = footprint.chunk({"time": 2, "H_back": -1})
    flux = flux.astype(np.float64).chunk({"time": 8})
    with pytest.raises(ValueError, match="share one float32 or float64 dtype"):
        fp_x_flux_time_resolved_numba_core(footprint, flux)
    result = fp_x_flux_time_resolved_numba(footprint.astype(np.int64), flux, cast_float32=False)
    assert result.dtype == np.float64
    xr.testing.assert_allclose(result.compute(), _reference(footprint.astype(np.int64), flux).compute())


@pytest.mark.parametrize("shift, fillna_zero", [(1e-6, True), (1e-3, True), (1e-3, False)])
def test_spatial_alignment_has_bounded_nearest_tolerance(shift: float, fillna_zero: bool) -> None:
    """Small shifts preserve edge values and distant grids obey missing policy."""
    footprint, flux = _inputs()
    shifted = flux.assign_coords(
        lat=flux.lat.astype(np.float64) + shift, lon=flux.lon.astype(np.float64) + shift
    )
    result = fp_x_flux_time_resolved_numba(footprint, shifted, fillna_zero=fillna_zero).compute()
    if shift < 1e-5:
        xr.testing.assert_allclose(result, _reference(footprint, flux))
        assert (result.isel(lat=-1, lon=-1) > 0).all()
    elif fillna_zero:
        assert (result == 0).all()
    else:
        assert result.isnull().all()


def test_scaled_inputs_stay_lazy_and_unchanged_through_compute() -> None:
    """Graph construction performs no data tasks and never mutates inputs."""
    from dask.callbacks import Callback

    footprint, flux = _inputs()
    footprint = footprint.chunk({"time": 2, "H_back": 1})
    flux = flux.chunk({"time": 5})
    footprint["fp_residual"].attrs["units"] = "m2 s umol-1"
    flux.attrs["units"] = "umol m-2 s-1"
    before_fp, before_flux = footprint.copy(deep=True), flux.copy(deep=True)
    graphs = [a.data.name for a in (footprint.fp_time_resolved, footprint.fp_residual, flux)]
    chunks = footprint.chunks, flux.chunks
    tasks = []
    with Callback(pretask=lambda *args: tasks.append(args[0])):
        result = fp_x_flux_time_resolved_numba(footprint, flux, time_chunk=2)
    assert tasks == []
    result.compute()
    xr.testing.assert_identical(footprint, before_fp)
    xr.testing.assert_identical(flux, before_flux)
    assert chunks == (footprint.chunks, flux.chunks)
    assert graphs == [a.data.name for a in (footprint.fp_time_resolved, footprint.fp_residual, flux)]


def test_scaled_flux_native_fraction_writes_one_ppm(tmp_path) -> None:
    """One micromole flux produces a native fraction before the ppm writer."""
    footprint, flux = _inputs()
    footprint["fp_time_resolved"] = xr.ones_like(footprint.fp_time_resolved) / 3
    footprint["fp_residual"] = xr.zeros_like(footprint.fp_residual)
    flux = xr.ones_like(flux)
    flux.attrs["units"] = "umol m-2 s-1"
    result = fp_x_flux_time_resolved_numba(footprint, flux)
    np.testing.assert_allclose(result.compute(), 1e-6)
    path = write_fp_x_flux_zarr(
        result,
        tmp_path / "scaled.zarr",
        provenance={"footprint": "test", "flux": "test", "observation_time_selector": "test"},
    )
    stored = xr.open_zarr(path)["fp_x_flux"]
    assert stored.dtype == np.float32
    assert stored.attrs["units"] == "ppm"
    np.testing.assert_allclose(stored, 1)


def test_regular_core_accepts_permuted_input_dimensions() -> None:
    """Dimension labels determine regular-kernel axes without eager copies."""
    footprint, flux = _inputs()
    expected = _reference(footprint, flux)
    footprint = footprint.transpose("lat", "lon", "time", "H_back", missing_dims="ignore").chunk(
        {"time": (2, 3), "H_back": -1}
    )
    flux = flux.transpose("source", "lon", "time", "lat").chunk({"time": 8})
    result = fp_x_flux_time_resolved_numba_core(footprint, flux)
    assert result.attrs["kernel"] == "numba_block"
    xr.testing.assert_allclose(result.compute(), expected)


def test_mixed_precision_is_applied_before_unit_scale_arithmetic() -> None:
    """Preserve float32 source values during scaling in common float64 precision."""
    footprint, flux = _inputs()
    footprint["fp_time_resolved"] = xr.full_like(footprint.fp_time_resolved, np.float32(1e-6))
    footprint["fp_time_resolved"].attrs["units"] = "m2 s umol-1"
    footprint["fp_residual"] = xr.zeros_like(footprint.fp_residual)
    flux = xr.ones_like(flux, dtype=np.float64)
    result = fp_x_flux_time_resolved_numba(footprint, flux, cast_float32=False)
    expected = 3 * (float(np.float32(1e-6)) * 1e6)
    assert result.dtype == np.float64
    np.testing.assert_allclose(result.compute(), expected, rtol=0, atol=1e-15)


@pytest.mark.parametrize("time_chunk", [1, 2])
@pytest.mark.parametrize("singleton", [False, True])
def test_regular_wrapper_preserves_small_requested_output_chunks(time_chunk: int, singleton: bool) -> None:
    """Use sufficient compute halo chunks before restoring small output chunks."""
    footprint, flux = _inputs()
    footprint = xr.concat(
        [footprint, footprint.assign_coords(time=footprint.time + np.timedelta64(5, "h"))], dim="time"
    )
    footprint = footprint.isel(H_back=[0, 2]).assign_coords(H_back=[0, 3])
    if singleton:
        footprint = footprint.isel(time=[0])
    result = fp_x_flux_time_resolved_numba(footprint, flux, time_chunk=time_chunk)
    assert max(result.chunksizes["time"]) <= time_chunk
    assert result.attrs["compute_time_chunk"] >= 3
    xr.testing.assert_allclose(result.compute(), _reference(footprint, flux))


def test_regular_core_preserves_small_prepared_chunks_using_indexed_kernel() -> None:
    """Prepared chunks smaller than lag depth use indexed computation unchanged."""
    footprint, flux = _inputs()
    footprint = footprint.isel(H_back=[0, 2]).assign_coords(H_back=[0, 3]).chunk({"time": 1, "H_back": -1})
    flux = flux.chunk({"time": 8})
    result = fp_x_flux_time_resolved_numba_core(footprint, flux)
    assert result.attrs["kernel"] == "numba_indexed"
    assert result.chunksizes["time"] == footprint.chunksizes["time"]
    xr.testing.assert_allclose(result.compute(), _reference(footprint, flux).compute())


@pytest.mark.parametrize("lags", [[np.nan], [np.inf], []])
@pytest.mark.parametrize("operator", [fp_x_flux_time_resolved_numba, fp_x_flux_time_resolved])
def test_invalid_lags_are_rejected_before_graph_construction(lags: list[float], operator) -> None:
    """Reject non-finite or absent lags before conversion to integer offsets."""
    footprint, flux = _inputs()
    footprint = footprint.isel(H_back=slice(0, len(lags))).assign_coords(H_back=lags)
    with pytest.raises(ValueError, match="at least one finite"):
        operator(footprint, flux)
