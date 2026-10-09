"""Numerical and missing-data regression cases for OpenGHG issue #1775."""

from math import fsum, sqrt

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from openghg.data_processing._resampling import (
    _weighted_resample,
    resampler,
    surface_obs_resampler,
    weighted_resample,
)


def _observations(mf, variability, counts, dtype=np.float64):
    """Make hourly observations while preserving the requested stored floating dtype."""
    return xr.Dataset(
        {
            "co2": ("time", np.asarray(mf, dtype=dtype)),
            "co2_variability": ("time", np.asarray(variability, dtype=dtype)),
            "co2_number_of_observations": ("time", np.asarray(counts, dtype=np.int64)),
        },
        coords={"time": pd.date_range("2020-01-01", periods=len(mf), freq="h")},
    )


def _population_reference(ds):
    """Pool complete summaries with centered math.fsum arithmetic on exactly stored inputs."""
    means = [float(value) for value in ds.co2.data]
    deviations = [float(value) for value in ds.co2_variability.data]
    counts = [int(value) for value in ds.co2_number_of_observations.data]
    count = sum(counts)
    mean = fsum(n * x for n, x in zip(counts, means)) / count
    variance = fsum(n * (s * s + (x - mean) ** 2) for n, x, s in zip(counts, means, deviations)) / count
    return mean, sqrt(variance), count


@pytest.mark.parametrize(
    "mf,variability,counts,dtype",
    [
        pytest.param(
            [333, 333],
            [0.02, 0.02],
            [1, 1],
            np.float32,
            id="float32-zero-spread",
        ),
        pytest.param(
            [333.05, 333.05],
            [0.02, 0.02],
            [1, 1],
            np.float32,
            id="float32-inflated-spread",
        ),
        pytest.param(
            [333, 333.01, 333.02, 333.03],
            [0.02] * 4,
            [1, 2, 3, 4],
            np.float32,
            id="float32-unequal-counts",
        ),
        pytest.param(
            [333, 333],
            [1e-6, 1e-6],
            [1, 1],
            np.float64,
            id="float64-tiny-variability",
        ),
        pytest.param([333, 333.01], [0.02, 0.02], [1, 2], np.float64, id="float64-control"),
        pytest.param(
            [0, 0.01],
            [0.02, 0.02],
            [1, 2],
            np.float32,
            id="float32-near-zero-control",
        ),
    ],
)
def test_weighted_variability_matches_centered_population(mf, variability, counts, dtype):
    """Retain supplied within-record spread when pooling constant or nearby concentrations.

    The oracle accounts for stored float32 quantisation rather than ideal decimal inputs.
    Float64 outputs are checked at a relative tolerance of 1e-12 against centered arithmetic.
    """
    ds = _observations(mf, variability, counts, dtype)
    expected_mean, expected_variability, expected_count = _population_reference(ds)
    result = weighted_resample(ds, averaging_period="4h", species="co2")

    assert result.co2_number_of_observations.item() == expected_count
    assert result.co2.item() == pytest.approx(expected_mean, rel=1e-12, abs=1e-15)
    assert result.co2_variability.item() == pytest.approx(expected_variability, rel=1e-12, abs=1e-15)


@pytest.mark.parametrize(
    "missing_count",
    [
        pytest.param(1, id="positive-count"),
        pytest.param(0, id="zero-count-control"),
    ],
)
def test_missing_mole_fraction_excludes_observation_weight(missing_count):
    """An unavailable mole fraction cannot contribute observations to a pooled summary."""
    ds = _observations([333, np.nan], [0.02, np.nan], [1, missing_count])
    result = weighted_resample(ds, averaging_period="2h", species="co2")

    assert result.co2_number_of_observations.item() == 1
    assert result.co2.item() == pytest.approx(333)
    assert result.co2_variability.item() == pytest.approx(0.02, rel=1e-7)


def test_partial_missing_variability_is_translation_invariant():
    """Shifting every concentration cannot change spread, regardless of missing-data policy."""
    ds = _observations([0, 0], [0.02, np.nan], [1, 1])
    shifted = ds.assign(co2=ds.co2 + 333)
    original_result = weighted_resample(ds, averaging_period="2h", species="co2")
    shifted_result = weighted_resample(shifted, averaging_period="2h", species="co2")

    np.testing.assert_allclose(
        original_result.co2_variability.data,
        shifted_result.co2_variability.data,
        rtol=1e-7,
        atol=1e-12,
        equal_nan=True,
    )


def test_population_pooling_matches_expanded_observations_and_nested_bins():
    """Pool population summaries to the same result as explicit samples and aligned intermediate bins."""
    groups = [[1, 3], [4, 4, 4], [5], [6, 8]]
    ds = _observations(
        [np.mean(group) for group in groups],
        [np.std(group, ddof=0) for group in groups],
        [len(group) for group in groups],
    )
    expanded = [value for group in groups for value in group]
    result = weighted_resample(ds, averaging_period="4h", species="co2")
    intermediate = weighted_resample(ds, averaging_period="2h", species="co2")
    nested = weighted_resample(intermediate, averaging_period="4h", species="co2")

    for resampled in (result, nested):
        assert resampled.co2.item() == pytest.approx(np.mean(expanded), rel=1e-12)
        assert resampled.co2_variability.item() == pytest.approx(np.std(expanded, ddof=0), rel=1e-12)
        assert resampled.co2_number_of_observations.item() == len(expanded)
    xr.testing.assert_allclose(result, nested)


@pytest.mark.parametrize("supplied_variability", [False, True], ids=["absent", "all-missing"])
def test_missing_variability_uses_weighted_concentration_spread(supplied_variability):
    """Fallback spread respects observation counts even when all supplied deviations are missing."""
    ds = _observations([1, 3], [np.nan, np.nan], [1, 3])
    if not supplied_variability:
        ds = ds.drop_vars("co2_variability")
    result = surface_obs_resampler(ds, averaging_period="2h", species="co2")

    assert result.co2.item() == pytest.approx(2.5)
    assert result.co2_variability.item() == pytest.approx(sqrt(0.75))
    assert result.co2_number_of_observations.item() == 4


def test_partial_variability_pools_known_spread_across_nested_bins():
    """Missing deviations add no known within-record variance while all valid means contribute spread."""
    ds = _observations([1, 3, 5, 7], [0.2, np.nan, np.nan, 0.4], [1, 3, 2, 2])
    expanded_means = [1, 3, 3, 3, 5, 5, 7, 7]
    mean = fsum(expanded_means) / len(expanded_means)
    # Each supplied deviation contributes its count times its population variance.
    variance = (fsum((value - mean) ** 2 for value in expanded_means) + 0.2**2 + 2 * 0.4**2) / 8
    direct = weighted_resample(ds, averaging_period="4h", species="co2")
    intermediate = weighted_resample(ds, averaging_period="2h", species="co2")
    nested = weighted_resample(intermediate, averaging_period="4h", species="co2")

    for result in (direct, nested):
        assert result.co2.item() == pytest.approx(mean, rel=1e-12)
        assert result.co2_variability.item() == pytest.approx(sqrt(variance), rel=1e-12)
        assert result.co2_number_of_observations.item() == len(expanded_means)


def test_weighted_resampling_preserves_lazy_labels_and_attrs():
    """Lazy multi-station resampling retains coordinates and metadata without mixing stations."""
    from dask import is_dask_collection

    ds = _observations([1, 3], [np.nan, np.nan], [1, 3])
    ds = xr.concat([ds, ds.assign(co2=ds.co2 + 332)], dim=pd.Index(["near-zero", "offset"], name="station"))
    ds.attrs = {"site": "test"}
    ds.co2.attrs = {"units": "ppm"}
    ds.co2_variability.attrs = {"units": "ppm", "long_name": "variability"}
    ds.co2_number_of_observations.attrs = {"long_name": "number of observations"}
    ds = ds.chunk({"time": 1, "station": 1})
    result = weighted_resample(ds, averaging_period="2h", species="co2")

    for variable in ds.data_vars:
        assert is_dask_collection(result[variable].data)
        assert result[variable].attrs == ds[variable].attrs
        assert set(result[variable].dims) == {"time", "station"}
    assert ds.attrs.items() <= result.attrs.items()
    xr.testing.assert_identical(result.station, ds.station)
    np.testing.assert_array_equal(result.time.data, ds.time.data[:1])
    computed = result.compute()
    np.testing.assert_allclose(computed.co2.sel(station="near-zero"), [2.5])
    np.testing.assert_allclose(computed.co2.sel(station="offset"), [334.5])
    np.testing.assert_allclose(computed.co2_variability, sqrt(0.75))
    assert bool((computed.co2_number_of_observations == 4).all())


def test_empty_bins_and_nonfinite_mole_fractions_have_no_weight():
    """Empty periods stay missing and infinite concentrations cannot dilute a valid summary."""
    ds = _observations([1, np.inf, 3], [np.nan] * 3, [1, 10, 3])
    ds = ds.assign_coords(time=pd.to_datetime(["2020-01-01T00:00", "2020-01-01T04:00", "2020-01-01T05:00"]))
    result = weighted_resample(ds, averaging_period="2h", species="co2")

    np.testing.assert_allclose(result.co2, [1, np.nan, 3], equal_nan=True)
    np.testing.assert_allclose(result.co2_variability, [0, np.nan, 0], equal_nan=True)
    np.testing.assert_allclose(result.co2_number_of_observations, [1, np.nan, 3], equal_nan=True)


@pytest.mark.parametrize("variability_route", ["explicit", "remainder"])
def test_explicit_resampler_routing_keeps_variability_separate(variability_route):
    """Custom routing can combine weighted means with independently requested variability handling."""
    ds = _observations([1, 3], [0.2, 0.4], [1, 3])
    funcs = {"weighted": ["co2", "co2_number_of_observations"]}
    if variability_route == "explicit":
        ds = ds.drop_vars("co2_variability")
        funcs["variability"] = ["co2"]
        expected_variability = 1.0
    else:
        # The supplied variability is left to the generic mean-resampled remainder.
        expected_variability = 0.3
    result = resampler(ds, averaging_period="2h", func_dict=funcs, species="co2")

    assert result.co2.item() == pytest.approx(2.5)
    assert result.co2_number_of_observations.item() == 4
    assert result.co2_variability.item() == pytest.approx(expected_variability)


def test_invalid_counts_have_no_weight_and_all_invalid_bins_stay_missing():
    """Finite concentrations with nonpositive or nonfinite counts supply no usable population weight."""
    ds = _observations([1] + [3] * 8, [0.2] * 9, [1] * 9)
    ds["co2_number_of_observations"] = (
        "time",
        np.asarray([1, 0, -1, np.nan, np.inf, 0, -1, np.nan, np.inf]),
    )
    result = weighted_resample(ds, averaging_period="5h", species="co2")

    np.testing.assert_allclose(result.co2, [1, np.nan], equal_nan=True)
    np.testing.assert_allclose(result.co2_variability, [0.2, np.nan], equal_nan=True)
    np.testing.assert_allclose(result.co2_number_of_observations, [1, np.nan], equal_nan=True)


def test_weighted_resampling_uses_month_end_bin_labels():
    """The pooling helper assigns irregular observations to their right-labelled calendar month."""
    ds = _observations([1, 3, 5, 7], [0.2] * 4, [1, 3, 2, 2])
    ds = ds.assign_coords(
        time=pd.to_datetime(
            ["2020-01-30", "2020-01-31T12:00", "2020-02-01", "2020-02-29T12:00"],
            format="mixed",
        )
    )
    # The public decorator requires fixed durations; exercise calendar bins in the pooling helper.
    result = _weighted_resample(
        ds.co2,
        ds.co2_number_of_observations,
        averaging_period="ME",
        mf_variability=ds.co2_variability,
        species="co2",
    )

    np.testing.assert_array_equal(result.time.data, pd.to_datetime(["2020-01-31", "2020-02-29"]).to_numpy())
    np.testing.assert_allclose(result.co2, [2.5, 6], rtol=1e-12)
    np.testing.assert_allclose(result.co2_variability, [sqrt(0.79), sqrt(1.04)], rtol=1e-12)
    np.testing.assert_array_equal(result.co2_number_of_observations, [4, 4])
