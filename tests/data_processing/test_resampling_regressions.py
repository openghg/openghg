"""Numerical and missing-data regression cases for OpenGHG issue #1775."""

from math import fsum, sqrt

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from openghg.data_processing._resampling import weighted_resample


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

    The oracle uses the stored float32 values rather than ideal decimal inputs. A relative
    tolerance of 1e-7 permits float32 output rounding without accepting loss of variability.
    """
    ds = _observations(mf, variability, counts, dtype)
    expected_mean, expected_variability, expected_count = _population_reference(ds)
    result = weighted_resample(ds, averaging_period="4h", species="co2")

    assert result.co2_number_of_observations.item() == expected_count
    assert result.co2.item() == pytest.approx(expected_mean, rel=1e-7, abs=1e-12)
    assert result.co2_variability.item() == pytest.approx(expected_variability, rel=1e-7, abs=1e-12)


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
