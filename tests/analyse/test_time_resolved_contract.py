"""Independent native-interval checks shared by both time-resolved operators."""

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from openghg.analyse import fp_x_flux_time_resolved_numba
from openghg.analyse._modelled_obs import fp_x_flux_time_resolved


def _inputs(releases, flux_times, lags=(0, 3)):
    releases = pd.DatetimeIndex(releases)
    flux_times = pd.DatetimeIndex(flux_times)
    fp = xr.Dataset(
        {
            "fp_time_resolved": (
                ("time", "lat", "lon", "H_back"),
                np.broadcast_to(np.arange(1, len(lags) + 1, dtype=float), (len(releases), 1, 1, len(lags))),
            ),
            "fp_residual": (("time", "lat", "lon"), np.full((len(releases), 1, 1), 0.5)),
        },
        coords={"time": releases, "lat": [50.0], "lon": [0.0], "H_back": list(lags)},
    )
    flux = xr.DataArray(
        (np.arange(len(flux_times), dtype=float) ** 2 + 1).reshape(-1, 1, 1),
        dims=("time", "lat", "lon"),
        coords={"time": flux_times, "lat": fp.lat, "lon": fp.lon},
    )
    return fp, flux


def _oracle(fp, flux):
    """Evaluate each lag with a plain interval-membership loop."""
    step = flux.time.values[1] - flux.time.values[0]
    lags = fp.H_back.values
    fallback = step > np.timedelta64(int(max(lags)) + 1, "h")
    values = []
    for release in fp.time.values:
        month = pd.Timestamp(release).to_period("M")
        monthly = [
            float(flux.values[i, 0, 0])
            for i, stamp in enumerate(flux.time.values)
            if pd.Timestamp(stamp).to_period("M") == month
        ]
        monthly_mean = sum(monthly) / len(monthly)
        total = 0.5 * monthly_mean
        for weight, lag in enumerate(lags, start=1):
            target = release - np.timedelta64(int(lag), "h")
            hits = [
                float(flux.values[i, 0, 0])
                for i, stamp in enumerate(flux.time.values)
                if stamp <= target < stamp + step
            ]
            assert len(hits) == 1
            total += weight * (monthly_mean if fallback else hits[0])
        values.append(total)
    return values


def _check_both(fp, flux):
    expected = _oracle(fp, flux)
    lazy_flux = flux.chunk({"time": 5})
    reference = fp_x_flux_time_resolved(fp, lazy_flux)
    assert hasattr(reference.data, "__dask_graph__")
    xr.testing.assert_identical(reference.time, fp.time)
    np.testing.assert_allclose(reference.compute().values.ravel(), expected, rtol=1e-12)
    pytest.importorskip("numba")
    numerical = fp_x_flux_time_resolved_numba(fp, lazy_flux, cast_float32=False)
    assert hasattr(numerical.data, "__dask_graph__")
    xr.testing.assert_identical(numerical.time, fp.time)
    np.testing.assert_allclose(numerical.compute().values.ravel(), expected, rtol=1e-12)


@pytest.mark.parametrize("flux_step", [1, 2, 3, 4, 5, 6, 7])
@pytest.mark.parametrize("release_step", [2, 3])
@pytest.mark.parametrize("lags", [(0, 1, 2), (0, 3)])
def test_native_intervals_and_strict_fallback(flux_step, release_step, lags):
    """Sparse lags remain offsets and equality at the fallback cutoff stays resolved."""
    fp, flux = _inputs(
        pd.date_range("2024-01-02 12:37", periods=3, freq=f"{release_step}h"),
        pd.date_range("2024-01-01 00:15", periods=60, freq=f"{flux_step}h"),
        lags,
    )
    _check_both(fp, flux)


@pytest.mark.parametrize("start", ["2024-01-31 21:37", "2023-12-31 21:37"])
@pytest.mark.parametrize("flux_step", [1, 4, 5])
def test_release_calendar_month_means_include_all_supplied_values(start, flux_step):
    """Resolved and residual terms cross months with supplied flux outside releases."""
    release = pd.Timestamp(start)
    fp, flux = _inputs(
        pd.date_range(release, periods=4, freq="3h"),
        pd.date_range(release.normalize() - pd.Timedelta(days=2), periods=120, freq=f"{flux_step}h"),
    )
    _check_both(fp, flux)


@pytest.mark.parametrize("flux_step", [1, 4, 5, 7])
@pytest.mark.parametrize("bound", ["start", "end"])
def test_missing_native_interval_coverage_raises_before_compute(flux_step, bound):
    """Both resolved and monthly fallback branches reject uncovered lag halos."""
    flux_times = pd.date_range("2024-01-01 00:15", periods=8, freq=f"{flux_step}h")
    release = (
        flux_times[0] + pd.Timedelta(hours=3) - pd.Timedelta(nanoseconds=1)
        if bound == "start"
        else flux_times[-1] + pd.Timedelta(hours=flux_step)
    )
    fp, flux = _inputs([release], flux_times)
    for operator in (fp_x_flux_time_resolved, fp_x_flux_time_resolved_numba):
        with pytest.raises(ValueError, match="coverage"):
            operator(fp, flux.chunk({"time": 2}))


@pytest.mark.parametrize("flux_step", [1, 4, 5, 7])
@pytest.mark.parametrize("bound", ["start", "end"])
def test_native_interval_boundary_coverage_is_half_open(flux_step, bound):
    """The first lag may equal the start and the release may precede the end by 1 ns."""
    flux_times = pd.date_range("2024-01-01 00:15", periods=8, freq=f"{flux_step}h")
    release = (
        flux_times[0] + pd.Timedelta(hours=3)
        if bound == "start"
        else flux_times[-1] + pd.Timedelta(hours=flux_step) - pd.Timedelta(nanoseconds=1)
    )
    fp, flux = _inputs([release], flux_times)
    _check_both(fp, flux)


@pytest.mark.parametrize(
    "times",
    [
        ["2024-01-01", "2024-01-01 01:00", "2024-01-01 03:00"],
        ["2024-01-01", "2024-01-01 00:30", "2024-01-01 01:00"],
    ],
)
def test_unsupported_flux_cadence_raises_before_compute(times):
    """Irregular and fractional-hour native flux cadences are unsupported."""
    fp, flux = _inputs(["2024-01-01 03:00"], pd.to_datetime(times, format="mixed"))
    for operator in (fp_x_flux_time_resolved, fp_x_flux_time_resolved_numba):
        with pytest.raises(ValueError, match="regular with a whole-hour"):
            operator(fp, flux.chunk({"time": 2}))


def test_hand_calculated_sparse_lag_and_monthly_terms():
    """At 03:30, resolve 16 + 2*1 and add half the native-value mean 11."""
    fp, flux = _inputs(["2024-01-01 03:30"], pd.date_range("2024-01-01", periods=5, freq="h"))
    flux.data = np.array([1.0, 4.0, 9.0, 16.0, 25.0]).reshape(-1, 1, 1)
    assert _oracle(fp, flux) == [23.5]
    _check_both(fp, flux)


@pytest.mark.parametrize("flux_step", [1, 4, 5])
@pytest.mark.parametrize("hours", [[9, 3], [9, 3, 9]])
def test_release_order_and_duplicates_preserve_interval_values(flux_step, hours):
    """Slice by release extrema while retaining requested order and repeated releases."""
    fp, flux = _inputs(
        [pd.Timestamp("2024-01-01") + pd.Timedelta(hours=hour, minutes=30) for hour in hours],
        pd.date_range("2024-01-01", periods=20, freq=f"{flux_step}h"),
    )
    _check_both(fp, flux)


@pytest.mark.parametrize("empty_input", ["footprint", "flux"])
def test_empty_time_coordinates_raise_value_error(empty_input):
    """Reject empty releases or native intervals before slicing or building windows."""
    fp, flux = _inputs(["2024-01-01 09:30"], pd.date_range("2024-01-01", periods=20, freq="h"))
    if empty_input == "footprint":
        fp = fp.isel(time=slice(0, 0))
    else:
        flux = flux.isel(time=slice(0, 0))
    for operator in (fp_x_flux_time_resolved, fp_x_flux_time_resolved_numba):
        with pytest.raises(ValueError, match="time coordinate must not be empty"):
            operator(fp, flux)


@pytest.mark.parametrize("missing", ["fp_time_resolved", "fp_residual", "flux"])
def test_missing_physical_attrs_assume_canonical_units_without_mutation(missing):
    """Each absent declaration independently assumes canonical units before Pint multiplication."""
    fp, flux = _inputs(["2024-01-01 03:30"], pd.date_range("2024-01-01", periods=5, freq="h"))
    flux.data = np.array([1.0, 4.0, 9.0, 16.0, 25.0]).reshape(-1, 1, 1)
    for name in fp.data_vars:
        fp[name].attrs["units"] = "m2 s umol-1"
    flux.attrs["units"] = "umol m-2 s-1"
    if missing == "flux":
        del flux.attrs["units"]
    else:
        del fp[missing].attrs["units"]
    before_fp, before_flux = fp.copy(deep=True), flux.copy(deep=True)
    fp_scale = 1.0 if missing == "fp_time_resolved" else 1e6
    residual_scale = 1.0 if missing == "fp_residual" else 1e6
    flux_scale = 1.0 if missing == "flux" else 1e-6
    expected_ppm = (18.0 * fp_scale + 5.5 * residual_scale) * flux_scale * 1e6
    for operator in (fp_x_flux_time_resolved, fp_x_flux_time_resolved_numba):
        kwargs = {"cast_float32": False} if operator is fp_x_flux_time_resolved_numba else {}
        result = operator(fp, flux.chunk({"time": 2}), **kwargs)
        ppm = result.pint.quantify().pint.to("ppm").pint.dequantify().compute()
        np.testing.assert_allclose(ppm.values, expected_ppm, rtol=1e-12)
    xr.testing.assert_identical(fp, before_fp)
    xr.testing.assert_identical(flux, before_flux)


@pytest.mark.parametrize("flux_step", [4, 5, 7])
def test_release_month_requires_a_native_flux_sample(flux_step):
    """Valid native intervals cannot define a release-month mean from another month."""
    times = pd.date_range(end="2024-01-31 23:15", periods=2, freq=f"{flux_step}h")
    fp, flux = _inputs(["2024-02-01 00:15"], times)
    for operator in (fp_x_flux_time_resolved, fp_x_flux_time_resolved_numba):
        with pytest.raises(ValueError, match="month"):
            operator(fp, flux.chunk({"time": 1}))
