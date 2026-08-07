from __future__ import annotations

import json

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from openghg.analyse import (
    fp_x_flux_time_resolved_numba,
    warm_numba_fp_x_flux,
    write_fp_x_flux_zarr,
)
from openghg.analyse._modelled_obs import fp_x_flux_time_resolved


@pytest.fixture(scope="module", autouse=True)
def data_read() -> None:
    """Override the analyse integration-data fixture for these unit tests."""


def _inputs() -> tuple[xr.Dataset, xr.DataArray]:
    """Create representative footprint and source-resolved flux inputs.

    Returns:
        A footprint Dataset and source-resolved flux DataArray for unit tests.
    """
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
    """Calculate the expected spatially resolved result directly with xarray.

    Args:
        footprint: Time-resolved and residual footprint data.
        flux: Source-resolved flux data.

    Returns:
        The directly calculated source-resolved result.
    """
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
    """Calculate a source-resolved result with the existing operator.

    Args:
        footprint: Time-resolved and residual footprint data.
        flux: Flux data with or without a source dimension.

    Returns:
        Existing operator results combined along the source dimension.
    """
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


def test_fp_x_flux_time_resolved_numba_matches_reference_and_preserves_source_metadata() -> None:
    """Match direct values while preserving source-dependent coordinates."""
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
    """Match the existing time-resolved operator for each source."""
    footprint, flux = _inputs()

    result = fp_x_flux_time_resolved_numba(footprint, flux).compute()
    expected = _existing_time_resolved_reference(footprint, flux)

    _assert_value_parity(result, expected.astype(np.float32))


def test_time_selector_is_applied_to_finished_lazy_result() -> None:
    """Apply observation-time selection after constructing the lazy result."""
    footprint, flux = _inputs()
    selected = footprint["time"].values[[1, 3]]

    result = fp_x_flux_time_resolved_numba(footprint, flux, time_selector=selected, time_chunk=2)

    assert result["time"].values.tolist() == selected.tolist()
    xr.testing.assert_allclose(result.compute(), _reference(footprint, flux).sel(time=selected))


def test_sources_can_be_selected_and_relabelled() -> None:
    """Select requested sources and replace their output labels."""
    footprint, flux = _inputs()

    result = fp_x_flux_time_resolved_numba(
        footprint, flux, sources=["ff"], source_labels=["fossil_fuel"]
    )

    assert result["source"].values.tolist() == ["fossil_fuel"]
    assert result["sector"].values.tolist() == ["FF"]
    expected = _reference(footprint, flux.sel(source=["ff"])).assign_coords(source=["fossil_fuel"])
    xr.testing.assert_allclose(result.compute(), expected)


def test_single_source_flux_without_source_dimension_is_supported() -> None:
    """Support flux inputs without an explicit source dimension."""
    footprint, flux = _inputs()
    single_flux = flux.sel(source="bio", drop=True)

    result = fp_x_flux_time_resolved_numba(footprint, single_flux, source_labels=["biosphere"])
    expected = fp_x_flux_time_resolved(footprint, single_flux).expand_dims(source=["biosphere"])

    assert result.dims == ("source", "lat", "lon", "time")
    xr.testing.assert_allclose(result.compute(), expected.transpose(*result.dims).astype(np.float32))


@pytest.mark.parametrize("step_hours", [2, 24])
def test_regular_coarse_flux_matches_existing_time_resolved_method(step_hours: int) -> None:
    """Match the existing operator for regularly spaced coarse flux.

    Args:
        step_hours: Flux time step used by the parameterized test case.
    """
    footprint, flux = _inputs()
    end = flux["time"].values[-1] + np.timedelta64(1, "h")
    coarse_time = pd.date_range(flux["time"].values[0], end, freq=f"{step_hours}h", inclusive="left")
    coarse_flux = flux.reindex(time=coarse_time, method="nearest")

    result = fp_x_flux_time_resolved_numba(footprint, coarse_flux).compute()
    expected = _existing_time_resolved_reference(footprint, coarse_flux)

    _assert_value_parity(result, expected.astype(np.float32))


def test_time_selector_rejects_times_outside_footprint_grid() -> None:
    """Reject selected timestamps that are absent from the result grid."""
    footprint, flux = _inputs()

    with pytest.raises(ValueError, match="outside the result time grid"):
        fp_x_flux_time_resolved_numba(footprint, flux, time_selector=["2022-01-01"])


def test_flux_must_include_complete_lag_halo() -> None:
    """Require flux coverage extending through the full lag halo."""
    footprint, flux = _inputs()
    flux = flux.sel(time=slice(footprint["time"].values[0], None))

    with pytest.raises(ValueError, match="complete footprint lag halo"):
        fp_x_flux_time_resolved_numba(footprint, flux)


def test_write_fp_x_flux_zarr_writes_ppm_and_manifest(tmp_path) -> None:
    """Write ppm values and required provenance to Zarr and JSON.

    Args:
        tmp_path: Pytest-provided temporary directory.
    """
    footprint, flux = _inputs()
    result = fp_x_flux_time_resolved_numba(
        footprint, flux, time_selector=footprint["time"].values[[0, 2]]
    )
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
    assert manifest["operator_version"] == 1
    assert manifest["source"] == ["bio", "ff"]
    assert manifest["provenance"]["flux"]["checksum"] == "test-flux"


def test_warm_numba_fp_x_flux() -> None:
    """Compile the Numba kernel and return its readiness message."""
    assert warm_numba_fp_x_flux() == "numba fp_x_flux kernel ready"
