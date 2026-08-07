from __future__ import annotations

import json

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from openghg.analyse import (
    align_flux_to_time_targets,
    fp_x_flux_keep_space,
    fp_x_flux_keep_space_core,
    warm_numba_fp_x_flux,
    write_fp_x_flux_keep_space_zarr,
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
    """Calculate expected hourly keep-space values directly with xarray."""
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
        flux_lag = align_flux_to_time_targets(flux, targets).assign_coords(time=fp_lag["time"])
        term = fp_lag * flux_lag
        resolved = term if resolved is None else resolved + term
    low_frequency = flux.resample(time="1MS").mean().reindex(time=footprint["time"], method="ffill")
    return (resolved + footprint["fp_residual"] * low_frequency).transpose("source", "lat", "lon", "time")


def test_fp_x_flux_keep_space_matches_reference_and_preserves_source_metadata() -> None:
    """Match direct values and preserve source metadata coordinates."""
    footprint, flux = _inputs()
    result = fp_x_flux_keep_space(footprint, flux, time_chunk=2, lat_chunk=1, lon_chunk=1, source_chunk=1)

    assert hasattr(result.data, "__dask_graph__")
    assert result.dims == ("source", "lat", "lon", "time")
    assert result.dtype == np.float32
    assert result.attrs["units"] == "1"
    assert result["species"].values.tolist() == ["co2", "co2"]
    assert result["scenario"].values.tolist() == ["BASE", "BASE"]
    assert result["sector"].values.tolist() == ["GPP", "FF"]
    xr.testing.assert_allclose(result.compute(), _reference(footprint, flux))


def test_fp_x_flux_keep_space_matches_existing_time_resolved_method() -> None:
    """Match the existing time-resolved operator on its supported domain."""
    footprint, flux = _inputs()

    result = fp_x_flux_keep_space(footprint, flux).compute()
    expected = _existing_time_resolved_reference(footprint, flux)

    _assert_value_parity(result, expected.astype(np.float32))


def test_time_selector_is_applied_to_finished_lazy_result() -> None:
    """Apply observation-time selection only after graph construction."""
    footprint, flux = _inputs()
    selected = footprint["time"].values[[1, 3]]

    result = fp_x_flux_keep_space(footprint, flux, time_selector=selected, time_chunk=2)

    assert result["time"].values.tolist() == selected.tolist()
    xr.testing.assert_allclose(result.compute(), _reference(footprint, flux).sel(time=selected))


def test_sources_can_be_selected_and_relabelled() -> None:
    """Select and relabel source coordinates without changing values."""
    footprint, flux = _inputs()

    result = fp_x_flux_keep_space(footprint, flux, sources=["ff"], source_labels=["fossil_fuel"])

    assert result["source"].values.tolist() == ["fossil_fuel"]
    assert result["sector"].values.tolist() == ["FF"]
    expected = _reference(footprint, flux.sel(source=["ff"])).assign_coords(source=["fossil_fuel"])
    xr.testing.assert_allclose(result.compute(), expected)


def test_single_source_flux_without_source_dimension_is_supported() -> None:
    """Promote flux without a source dimension to one labelled source."""
    footprint, flux = _inputs()
    single_flux = flux.sel(source="bio", drop=True)

    result = fp_x_flux_keep_space(footprint, single_flux, source_labels=["biosphere"])
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

    result = fp_x_flux_keep_space(footprint, coarse_flux).compute()
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

    result = fp_x_flux_keep_space(
        footprint,
        flux,
        time_chunk=2,
        lat_chunk=1,
        lon_chunk=1,
        source_chunk=1,
    )

    assert result.attrs["kernel"] == "numba_indexed_keep_space"
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

    result = fp_x_flux_keep_space(footprint, flux)

    assert result.attrs["kernel"] == "numba_indexed_keep_space"
    xr.testing.assert_allclose(result.compute(), _interval_start_reference(footprint, flux))


def test_irregular_release_times_support_regular_coarse_flux() -> None:
    """Forward-fill coarse intervals before indexed irregular-time lookup."""
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
    coarse_flux = flux.isel(time=slice(None, None, 2))

    result = fp_x_flux_keep_space(footprint, coarse_flux)

    assert result.attrs["kernel"] == "numba_indexed_keep_space"
    xr.testing.assert_allclose(result.compute(), _interval_start_reference(footprint, coarse_flux))


def test_irregular_times_match_legacy_where_flux_grid_has_the_same_offset() -> None:
    """Match legacy values where its shifted time grid is scientifically valid."""
    footprint, flux = _inputs()
    release_times = pd.to_datetime(
        [
            "2021-01-01 03:00",
            "2021-01-01 05:00",
            "2021-01-01 06:00",
            "2021-01-01 09:00",
            "2021-01-01 11:00",
        ]
    )
    footprint = footprint.assign_coords(time=release_times)

    result = fp_x_flux_keep_space(footprint, flux).compute()
    expected = _existing_time_resolved_reference(footprint, flux)

    _assert_value_parity(result, expected.astype(np.float32))


def test_regular_hourly_release_times_keep_existing_numba_kernel() -> None:
    """Retain the regular halo kernel for consecutive hourly releases."""
    footprint, flux = _inputs()

    result = fp_x_flux_keep_space(footprint, flux)

    assert result.attrs["kernel"] == "numba_block_keep_space"


def test_irregular_release_times_default_to_bounded_time_chunks() -> None:
    """Default irregular computation to bounded release-time chunks."""
    footprint, flux = _inputs()
    footprint = footprint.isel(time=np.zeros(40, dtype=int)).assign_coords(
        time=pd.date_range("2021-01-01 03:01", periods=40, freq="53min")
    )

    result = fp_x_flux_keep_space(footprint, flux)

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

    core_result = fp_x_flux_keep_space_core(footprint, flux)
    wrapped_result = fp_x_flux_keep_space(
        footprint,
        flux,
        time_chunk=2,
        lat_chunk=1,
        lon_chunk=1,
        source_chunk=1,
    )

    assert core_result.attrs["kernel"] == "numba_indexed_keep_space"
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
        fp_x_flux_keep_space_core(footprint, flux)


def test_wrapper_sorts_hourly_releases_for_core_then_restores_input_order() -> None:
    """Sort for core computation and restore the caller's release order."""
    footprint, flux = _inputs()
    order = [0, 2, 1, 4, 3]
    footprint = footprint.isel(time=order)

    result = fp_x_flux_keep_space(footprint, flux, time_chunk=2)
    expected = _reference(footprint, flux)

    assert result.attrs["kernel"] == "numba_block_keep_space"
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

    result = fp_x_flux_keep_space(footprint, flux, time_selector=selected)
    expected = _interval_start_reference(footprint, flux).sel(time=selected)

    assert result["time"].values.tolist() == selected.tolist()
    xr.testing.assert_allclose(result.compute(), expected)


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

    result = fp_x_flux_keep_space(footprint, flux, source_labels=["biosphere"])
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

    result = fp_x_flux_keep_space(footprint, flux).compute()

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

    result = fp_x_flux_keep_space(footprint, flux).compute()

    np.testing.assert_allclose(result.values.reshape(-1), [2.0, 3.0])


def test_flux_time_labels_must_describe_interval_starts() -> None:
    """Reject flux explicitly labelled with non-start timestamps."""
    footprint, flux = _inputs()
    flux["time"].attrs["label"] = "right"

    with pytest.raises(ValueError, match="must label the start"):
        fp_x_flux_keep_space(footprint, flux)


def test_time_selector_rejects_times_outside_footprint_grid() -> None:
    """Reject selected timestamps absent from the footprint releases."""
    footprint, flux = _inputs()

    with pytest.raises(ValueError, match="outside the result time grid"):
        fp_x_flux_keep_space(footprint, flux, time_selector=["2022-01-01"])


def test_flux_must_include_complete_lag_halo() -> None:
    """Reject flux that omits any release-minus-lag target interval."""
    footprint, flux = _inputs()
    flux = flux.sel(time=slice(footprint["time"].values[0], None))

    with pytest.raises(ValueError, match="complete footprint lag halo"):
        fp_x_flux_keep_space(footprint, flux)


def test_write_fp_x_flux_keep_space_zarr_writes_ppm_and_manifest(tmp_path) -> None:
    """Persist float32 ppm values and the required provenance manifest."""
    footprint, flux = _inputs()
    result = fp_x_flux_keep_space(footprint, flux, time_selector=footprint["time"].values[[0, 2]])
    path = tmp_path / "cache.zarr"

    returned = write_fp_x_flux_keep_space_zarr(
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


def test_warm_numba_fp_x_flux() -> None:
    """Compile both Numba kernels and return the readiness message."""
    assert warm_numba_fp_x_flux() == "numba fp_x_flux kernel ready"
