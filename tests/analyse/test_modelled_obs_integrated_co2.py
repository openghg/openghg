"""Tests for integrated CO2 modelled-observation behaviour."""

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from openghg.analyse import ModelScenario, make_integrated_low_freq_flux
from openghg.analyse._modelled_obs import fp_x_flux_integrated
from openghg.dataobjects import FluxData, FootprintData, ObsData


def test_convert_units_preserves_unitless_observation_counts():
    """Converting mole fractions to ppm must not alter unitless observation counts."""
    dataset = xr.Dataset(
        {
            "mf": ("time", [1.0]),
            "mf_number_of_observations": ("time", [2]),
        },
        coords={"time": pd.to_datetime(["2012-01-01"])},
    )
    dataset.mf.attrs["units"] = "1"
    dataset.mf_number_of_observations.attrs["units"] = "1"
    original = dataset.copy(deep=True)

    converted = ModelScenario().convert_units(dataset, output_units="ppm")

    xr.testing.assert_identical(dataset, original)
    xr.testing.assert_identical(
        converted.mf_number_of_observations, original.mf_number_of_observations
    )
    assert converted.mf.item() == pytest.approx(1_000_000)


def test_add_footprint_forwards_time_resolved_selector(monkeypatch):
    """Footprint keyword retrieval must distinguish integrated and time-resolved data."""
    captured_keywords = None
    captured_data_type = None

    def capture_keywords(self, keywords, data_type):
        """Capture retrieval keywords without accessing an object store."""
        nonlocal captured_keywords, captured_data_type
        captured_keywords = keywords
        captured_data_type = data_type
        return None

    monkeypatch.setattr(ModelScenario, "_get_data", capture_keywords)
    scenario = ModelScenario()

    scenario.add_footprint(
        site="tac",
        fp_inlet="100m",
        domain="TEST",
        species="co2",
        time_resolved=False,
    )

    assert captured_data_type == "footprint"
    assert captured_keywords is not None
    assert all(keywords["time_resolved"] is False for keywords in captured_keywords)


@pytest.fixture
def integrated_co2_scenario():
    """Create a minimal integrated-CO2 scenario for modelled-observation tests."""
    obs = ObsData(
        data=xr.Dataset(
            {"mf": ("time", np.ones(2))},
            coords={"time": pd.to_datetime(["2012-01-01 00:00", "2012-01-01 01:00"])},
            attrs={"species": "co2", "site": "TEST_SITE", "inlet": "10m"},
        ),
        metadata={"species": "co2", "site": "TEST_SITE", "inlet": "10m"},
    )

    footprint = FootprintData(
        data=xr.Dataset(
            {"fp": (("time", "lat", "lon"), np.ones((2, 1, 1)))},
            coords={
                "time": pd.to_datetime(["2012-01-01 00:00", "2012-01-01 01:00"]),
                "lat": [1.0],
                "lon": [10.0],
            },
        ),
        metadata={"species": "co2", "site": "TEST_SITE", "inlet": "10m", "domain": "TESTDOMAIN"},
    )
    footprint.data.fp.attrs["units"] = "m2 s mol-1"
    footprint.data.lat.attrs["units"] = "degrees_north"
    footprint.data.lon.attrs["units"] = "degrees_east"

    flux = FluxData(
        data=xr.Dataset(
            {"flux": (("time", "lat", "lon"), np.array([[[1.0]], [[3.0]], [[5.0]]]))},
            coords={
                "time": pd.to_datetime(["2011-12-31 23:00", "2012-01-01 00:00", "2012-01-01 02:00"]),
                "lat": [1.0],
                "lon": [10.0],
            },
        ),
        metadata={"species": "co2", "source": "TESTSOURCE", "domain": "TESTDOMAIN"},
    )
    flux.data.flux.attrs["units"] = "mol m-2 s-1"
    flux.data.lat.attrs["units"] = "degrees_north"
    flux.data.lon.attrs["units"] = "degrees_east"

    return ModelScenario(obs=obs, footprint=footprint, flux=flux)


def test_modelled_obs_integrated_co2_uses_integrated_pipeline(integrated_co2_scenario):
    """Integrated CO2 footprints use monthly-mean fluxes, not the HiTRes path."""
    scenario = integrated_co2_scenario
    combined = scenario.footprints_data_merge()

    assert "mf_mod" in combined
    assert "mf_mod_high_res" not in combined

    flux_combined = scenario.combine_flux_sources()
    flux_monthly = make_integrated_low_freq_flux(flux_combined)
    expected_flux = flux_combined.flux.resample({"time": "1MS"}).mean().to_dataset(name="flux")
    xr.testing.assert_allclose(flux_monthly, expected_flux)

    expected = (
        fp_x_flux_integrated(combined, flux_monthly)
        .pint.quantify()
        .sum(["lat", "lon"])
        .pint.dequantify()
    )
    xr.testing.assert_allclose(combined.mf_mod, expected)


def test_modelled_obs_integrated_co2_split_by_sectors_retains_total(integrated_co2_scenario):
    """Sector splitting retains integrated totals and labels the sectoral outputs."""
    scenario = integrated_co2_scenario
    second_flux = FluxData(
        data=scenario.fluxes["TESTSOURCE"].data.copy(deep=True),
        metadata={"species": "co2", "source": "TESTSOURCE2", "domain": "TESTDOMAIN"},
    )
    scenario.add_flux(flux=second_flux)

    combined = scenario.footprints_data_merge(
        calc_fp_x_flux=True,
        split_by_sectors=True,
        calc_bc=False,
    )

    assert combined.mf_mod.dims == ("time",)
    assert combined.mf_mod_sectoral.dims == ("source", "time")
    assert combined.fp_x_flux.dims == ("lat", "lon", "time")
    assert combined.fp_x_flux_sectoral.dims == ("source", "lat", "lon", "time")
    assert combined.source.values.tolist() == ["TESTSOURCE", "TESTSOURCE2"]
    xr.testing.assert_allclose(combined.mf_mod_sectoral.sum("source"), combined.mf_mod)
    xr.testing.assert_allclose(combined.fp_x_flux_sectoral.sum("source"), combined.fp_x_flux)
