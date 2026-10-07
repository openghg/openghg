"""Public observation-resampling regressions for issue #1775.

Uniform counts avoid choosing an unequal-weight fallback policy.
"""

import warnings

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from openghg.data_processing import column_obs_resampler, surface_obs_resampler
from openghg.dataobjects import ObsData
from openghg.retrieve import get_obs_surface


RESAMPLERS = [surface_obs_resampler, column_obs_resampler]


def _observations(counts=None, variability=None):
    """Build two equal-duration observations with independently known mean and spread."""
    ds = xr.Dataset(
        {"co2": ("time", [1.0, 3.0])},
        coords={"time": pd.date_range("2020-01-01", periods=2, freq="h")},
        attrs={"site": "tac", "species": "co2", "inlet": "54m", "scale": "WMOX2007"},
    )
    ds.co2.attrs = {"units": "ppm", "long_name": "mole fraction of carbon dioxide in air"}
    if counts is not None:
        ds["co2_number_of_observations"] = ("time", counts)
    if variability is not None:
        ds["co2_variability"] = ("time", variability)
        ds.co2_variability.attrs = {"units": "ppm"}
    return ds


def _assert_fallback(result, units="ppm"):
    """Assert the population spread and mean of observations 1 and 3."""
    assert result.sizes["time"] == 1
    np.testing.assert_allclose(result.co2, [2.0])
    assert "co2_variability" in result
    np.testing.assert_allclose(result.co2_variability, [1.0])
    np.testing.assert_array_equal(result.time, [np.datetime64("2020-01-01T00:00:00", "ns")])
    assert result.co2.attrs["units"] == units


@pytest.mark.parametrize("resample", RESAMPLERS, ids=["surface", "column"])
def test_missing_variability_without_counts_uses_population_spread(resample):
    """Absent count and variability fields use the documented equal-record fallback."""
    result = resample(_observations(), "2h", "co2")
    _assert_fallback(result)


@pytest.mark.parametrize("resample", RESAMPLERS, ids=["surface", "column"])
def test_missing_variability_with_uniform_counts_uses_population_spread(resample):
    """Uniform positive counts must not suppress the documented variability fallback."""
    result = resample(_observations(counts=[2, 2]), "2h", "co2")
    _assert_fallback(result)
    np.testing.assert_array_equal(result.co2_number_of_observations, [4])


@pytest.mark.parametrize("resample", RESAMPLERS, ids=["surface", "column"])
@pytest.mark.parametrize("counts", [None, [2, 2]], ids=["no-counts", "uniform-counts"])
def test_all_nan_variability_uses_population_spread(resample, counts):
    """An entirely missing variability field has the same fallback as an absent field."""
    result = resample(_observations(counts=counts, variability=[np.nan, np.nan]), "2h", "co2")
    _assert_fallback(result)


@pytest.mark.parametrize("resample", RESAMPLERS, ids=["surface", "column"])
def test_single_observation_retains_supplied_variability(resample):
    """Supplied within-record variability survives a singleton averaging window."""
    ds = _observations(counts=[2, 2], variability=[0.5, 0.5]).isel(time=slice(0, 1))
    result = resample(ds, "2h", "co2")
    assert result.sizes["time"] == 1
    np.testing.assert_allclose(result.co2, [1.0])
    np.testing.assert_allclose(result.co2_variability, [0.5])
    np.testing.assert_array_equal(result.co2_number_of_observations, [2])


@pytest.mark.parametrize(
    "counts",
    [None, pytest.param([2, 2])],
    ids=["no-counts", "uniform-counts"],
)
@pytest.mark.parametrize("uncertainty", [np.nan, -9.99], ids=["nan", "negative-sentinel"])
def test_surface_retrieval_replaces_unusable_variability(monkeypatch, counts, uncertainty):
    """Real retrieval sanitisation followed by averaging restores concentration spread.

    Only store lookup is mocked: uncertainty sanitisation, resampling, variable
    renaming, and unit assignment all run through the public retrieval function.
    """
    obs = ObsData(
        metadata={"site": "tac", "species": "co2", "data_type": "surface"},
        data=_observations(counts=counts, variability=[uncertainty, uncertainty]),
    )
    monkeypatch.setattr("openghg.retrieve._access._get_generic", lambda **kwargs: obs)
    result = get_obs_surface(site="TAC", species="co2", average="2h", rename_vars=False)
    assert result is not None
    _assert_fallback(result.data, units="1e-06")


@pytest.mark.parametrize(
    "counts,expected_counts",
    [([0, 0], [1, 1]), ([0, 3], [1, 3])],
    ids=["all-zero", "preserve-positive"],
)
def test_surface_standardisation_repairs_zero_counts(monkeypatch, tmp_path, caplog, counts, expected_counts):
    """Central surface ingestion repairs zero counts for finite observations and warns.

    Only configuration is redirected to an isolated temporary object store.
    Parsing, schema validation, storage, and datasource loading run normally.
    """
    from openghg.objectstore import get_datasource
    from openghg.standardise import standardise_surface

    bucket_path = tmp_path / "store"
    config = {
        "object_store": {"regression": {"path": str(bucket_path), "permissions": "rw"}},
        "user_id": "issue-1775-regression",
        "config_version": "2",
    }
    monkeypatch.delenv("OPENGHG_TUT_STORE", raising=False)
    monkeypatch.setattr("openghg.objectstore._local_store.read_local_config", lambda: config)
    monkeypatch.setattr("openghg.util._user.read_local_config", lambda: config)
    ds = _observations(counts=counts)
    ds.attrs.update(
        network="decc",
        instrument="crds",
        sampling_period="1h",
        calibration_scale="WMOX2007",
        data_owner="Test owner",
        data_owner_email="test@example.invalid",
    )
    with caplog.at_level("WARNING"), warnings.catch_warnings(record=True) as emitted_warnings:
        warnings.simplefilter("always")
        results = standardise_surface(
            data=ds,
            source_format="OPENGHG",
            site="tac",
            network="decc",
            store="regression",
            update_mismatch="from_definition",
        )
    assert len(results) == 1
    datasource = get_datasource(bucket=str(bucket_path), uuid=results[0]["uuid"], data_type="surface")
    with datasource.get_data(version="latest") as stored:
        np.testing.assert_array_equal(stored.co2_number_of_observations, expected_counts)
        np.testing.assert_array_equal(stored.co2, [1.0, 3.0])
    warning_messages = [record.getMessage() for record in caplog.records if record.levelno >= 30]
    warning_messages.extend(str(warning.message) for warning in emitted_warnings)
    assert any(
        "co2" in message and ("count" in message.lower() or "number_of_observations" in message)
        for message in warning_messages
    )
